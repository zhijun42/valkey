# Most test cases in this file would involve the following setup design:
#
# Replica 4 follows primary 0. Replica 7 follows primary 3, and they have
# only one slot. We migrate this slot to shard 0, thus making server 3 and 7
# become primary 0's new replicas. At this point all these 4 servers are
# in the same shard. Then we stop primary 0 to trigger failover, and test
# how the remaining three servers would behave.

# Allocate slot 0 to the last primary and evenly distribute the remaining
# slots to the remaining primaries.
proc my_slot_allocation {masters replicas} {
    set avg [expr double(16384) / [expr $masters-1]]
    set slot_start 1
    for {set j 0} {$j < $masters-1} {incr j} {
        set slot_end [expr int(ceil(($j + 1) * $avg) - 1)]
        R $j cluster addslotsrange $slot_start $slot_end
        set slot_start [expr $slot_end + 1]
    }
    R [expr $masters-1] cluster addslots 0
}

proc get_my_primary_peer {srv_idx} {
    set role_response [R $srv_idx role]
    set primary_ip [lindex $role_response 1]
    set primary_port [lindex $role_response 2]
    set primary_peer "$primary_ip:$primary_port"
    return $primary_peer
}

proc populate_data {} {
    # Write some data to primary 0, slot 1, make a small repl_offset.
    for {set i 0} {$i < 1024} {incr i} {
        R 0 incr key_991803
    }
    assert_equal {1024} [R 0 get key_991803]

    # Write some data to primary 3, slot 0, make a big repl_offset.
    for {set i 0} {$i < 10240} {incr i} {
        R 3 incr key_977613
    }
    assert_equal {10240} [R 3 get key_977613]

    # 10s, make sure primary 0 will hang in the save.
    R 0 config set rdb-key-save-delay 100000000
}

proc stop_primary_0 { type } {
    if {$type == "shutdown"} {
        # Shutdown primary 0.
        catch {R 0 shutdown nosave}
        return -1
    } elseif {$type == "sigstop"} {
        # Pause primary 0.
        set primary0_pid [s 0 process_id]
        pause_process $primary0_pid
        return $primary0_pid
    }
}

proc resume_primary_0 { type primary0_pid } {
    if {$type == "sigstop"} {
        resume_process $primary0_pid

        # Wait for the old primary to go online and become a replica.
        wait_for_condition 1000 50 {
            [s 0 role] eq {slave}
        } else {
            fail "The old primary was not converted into replica"
        }
    }
}

proc move_slot_0_from_primary_3_to_primary_0 {} {
    set addr "[srv 0 host]:[srv 0 port]"
    set src_node_id [R 3 CLUSTER MYID]
    set code [catch {
        exec src/valkey-cli {*}[valkeycli_tls_config "./tests"] --cluster rebalance $addr --cluster-weight $src_node_id=0
    } result]
    if {$code != 0} {
        fail "valkey-cli --cluster rebalance returns non-zero exit code, output below:\n$result"
    }
}

# Wait until all given nodes have the expected key/value pairs.
proc wait_for_key_consistent {nodes kv_pairs} {
    # Build one single boolean expression: [string equal [R n get key] val] && ...
    set terms {}
    foreach node $nodes {
        dict for {key val} $kv_pairs {
            # Use [list] to safely quote key/val into the format string
            lappend terms [format {[string equal [R %d get %s] %s]} \
                                   $node [list $key] [list $val]]
        }
    }
    set condition [join $terms { && }]

    wait_for_condition 1000 50 $condition else {
        fail "Keys not consistent"
    }
}

proc test_migrated_replica {type} {
    test "Migrated replica reports zero repl offset and rank, and fails to win election - $type" {
        # Validate that shard 3's primary and replica can convert to replicas after
        # they lose the last slot.
        R 3 config set cluster-replica-validity-factor 0
        R 7 config set cluster-replica-validity-factor 0
        R 3 config set cluster-allow-replica-migration yes
        R 7 config set cluster-allow-replica-migration yes

        populate_data
        move_slot_0_from_primary_3_to_primary_0

        # Ensure slot migration does happen
        set R0_id [R 0 CLUSTER MYID]
        wait_for_log_messages -3 [list "*Slot 0 is no longer being migrated to node $R0_id*"] 0 1000 10
        wait_for_log_messages -3 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R0_id*"] 0 1000 10
        wait_for_log_messages -7 [list "*Lost my last slot during slot migration. Reconfiguring myself as a replica of $R0_id*"] 0 1000 10

        # This step is necessary. After server 7 reconfigures itself to follow
        # primary 0, it will get blocked for a while and become unreachable to
        # all nodes. See the `test_blocked_replica_stale_state_race` test below
        # to learn more about this edge case.
        # We need to send an arbitrary command (like PING here) to it to wait for
        # it to become responsive.
        R 7 PING

        # Stop primary 0 to start a failover.
        set primary0_pid [stop_primary_0 $type]

        # Wait for the replica to become a primary, and make sure
        # the other primary become a replica.
        set R4_id [R 4 CLUSTER MYID]
        wait_for_log_messages -4 {"*Failover election won: I'm the new primary*"} 0 1000 10
        wait_for_log_messages -3 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R4_id*"] 0 1000 10
        wait_for_log_messages -7 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R4_id*"] 0 1000 10

        # The node may not be able to initiate an election in time due to
        # problems with cluster communication. If an election is initiated,
        # we make sure the offset of server 3 / 7 is 0.
        if {[count_log_message -3 "Start of election"] != 0} {
            verify_log_message -3 "*Start of election*offset 0*" 0
        }
        if {[count_log_message -7 "Start of election"] != 0} {
            verify_log_message -7 "*Start of election*offset 0*" 0
        }

        # Make sure the right replica gets the highest rank.
        verify_log_message -4 "*Start of election*rank #0*" 0

        # Wait for the cluster to be ok.
        wait_for_cluster_propagation_except_node 0

        # Make sure the key exists and is consistent.
        R 3 readonly
        R 7 readonly
        wait_for_key_consistent {3 4 7} {key_991803 1024 key_977613 10240}

        resume_primary_0 $type $primary0_pid
    }
} ;# proc

start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test_migrated_replica "shutdown"
} my_slot_allocation cluster_allocate_replicas ;# start_cluster

start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test_migrated_replica "sigstop"
} my_slot_allocation cluster_allocate_replicas ;# start_cluster

proc test_nonempty_replica {type} {
    test "New non-empty replica reports zero repl offset and rank, and fails to win election - $type" {
        R 7 config set cluster-replica-validity-factor 0
        R 7 config set cluster-allow-replica-migration yes

        populate_data
        move_slot_0_from_primary_3_to_primary_0
        # Make server 7 a replica of server 0.
        R 7 cluster replicate [R 0 cluster myid]

        # Stop primary 0 to start a failover.
        set primary0_pid [stop_primary_0 $type]

        # Wait for the replica to become a primary.
        set R4_id [R 4 CLUSTER MYID]
        wait_for_log_messages -4 {"*Failover election won: I'm the new primary*"} 0 1000 10
        wait_for_log_messages -7 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R4_id*"] 0 1000 10

        # The node may not be able to initiate an election in time due to
        # problems with cluster communication. If an election is initiated,
        # we make sure server 7 gets the lower rank and it's offset is 0.
        if {[count_log_message -7 "Start of election"] != 0} {
            verify_log_message -7 "*Start of election*offset 0*" 0
        }

        # Make sure the right replica gets the highest rank.
        verify_log_message -4 "*Start of election*rank #0*" 0

        # Wait for the cluster to be ok.
        wait_for_cluster_propagation_except_node 0

        # Make sure the key exists and is consistent.
        R 7 readonly
        wait_for_key_consistent {4 7} {key_991803 1024}

        resume_primary_0 $type $primary0_pid
    }
} ;# proc

start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test_nonempty_replica "shutdown"
} my_slot_allocation cluster_allocate_replicas ;# start_cluster

start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test_nonempty_replica "sigstop"
} my_slot_allocation cluster_allocate_replicas ;# start_cluster

proc test_sub_replica {type} {
    test "Sub-replica reports zero repl offset and rank, and fails to win election - $type" {
        R 3 config set cluster-replica-validity-factor 0
        R 7 config set cluster-replica-validity-factor 0
        R 3 config set cluster-allow-replica-migration yes
        R 7 config set cluster-allow-replica-migration no

        populate_data
        move_slot_0_from_primary_3_to_primary_0

        # Make sure server 3 and server 7 become a replica of primary 0.
        set addr "[srv 0 host]:[srv 0 port]"
        wait_for_condition 1000 50 {
            [get_my_primary_peer 3] eq $addr &&
            [get_my_primary_peer 7] eq $addr
        } else {
            puts "R 3 role: [R 3 role]"
            puts "R 7 role: [R 7 role]"
            fail "Server 3 and 7 role response has not changed"
        }

        # Make sure server 7 got a sub-replica log.
        set R0_id [R 0 CLUSTER MYID]
        verify_log_message -7 "*I'm a sub-replica! Reconfiguring myself as a replica of $R0_id*" 0

        # Stop primary 0 to start a failover.
        set primary0_pid [stop_primary_0 $type]

        # Wait for the replica to become a primary, and make sure
        # the other primary become a replica.
        set R4_id [R 4 CLUSTER MYID]
        wait_for_log_messages -4 {"*Failover election won: I'm the new primary*"} 0 1000 10
        wait_for_log_messages -3 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R4_id*"] 0 1000 10
        wait_for_log_messages -7 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R4_id*"] 0 1000 10

        # The node may not be able to initiate an election in time due to
        # problems with cluster communication. If an election is initiated,
        # we make sure the offset of server 3 / 7 is 0.
        if {[count_log_message -3 "Start of election"] != 0} {
            verify_log_message -3 "*Start of election*offset 0*" 0
        }
        if {[count_log_message -7 "Start of election"] != 0} {
            verify_log_message -7 "*Start of election*offset 0*" 0
        }

        # Make sure the right replica gets the highest rank.
        verify_log_message -4 "*Start of election*rank #0*" 0

        # Wait for the cluster to be ok.
        wait_for_cluster_propagation_except_node 0

        # Make sure the key exists and is consistent.
        R 3 readonly
        R 7 readonly
        wait_for_key_consistent {3 4 7} {key_991803 1024 key_977613 10240}

        resume_primary_0 $type $primary0_pid
    }
}

start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test_sub_replica "shutdown"
} my_slot_allocation cluster_allocate_replicas ;# start_cluster

start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test_sub_replica "sigstop"
} my_slot_allocation cluster_allocate_replicas ;# start_cluster


# This test setup is almost identical to the previous sub-replica test, except that
# the replica 7's config cluster-allow-replica-migration is set to yes here.
#
# This test reproduces a natural race condition in Valkey's replication handshake,
# where multiple replicas attempt to synchronize with the same primary at nearly
# the same time. It does not depend on artificial delay configurations.
#
# After migrating shard 3's last slot to primary 0, both primary 3 and its replica 7
# attempt to replicate from the new primary. Typically, the first replica to send its
# PSYNC request (node 3) successfully enrolls for replication, triggering the master
# to fork the RDB child (RDB_CHILD_TYPE_SOCKET). The second replica (node 7), arriving
# milliseconds later, misses the join window and remains stuck waiting for a PING reply.
#
# While node 7 is in REPL_STATE_RECEIVE_PING_REPLY, its main thread is blocked in
# receiveSynchronousResponse(), unable to process other cluster messages. This makes
# node 7 appear unreachable to the rest of the cluster for roughly
# (5 × cluster-node-timeout) until it times out and retries the sync.
#
# During this blocking period, we stop primary 0 to trigger a failover. Replica 4 wins
# the election and becomes the new primary for the shard. At this moment:
#   - Node 0 (stopped)
#   - Node 3 (replica of 4)
#   - Node 4 (new primary)
#   - Node 7 (still replicating from 0)
# all belong to the same shard, but node 7 remains in an outdated state.
#
# When node 7 times out and reconnects, its outbound links are still valid, so it sends
# PINGs and receives a PONG from node 4, correctly identifying 4 as the new primary.
# However, inbound connections were previously closed by peers while node 7 was blocked.
# As node 7 reestablishes inbound sockets, it may receive delayed, stale cluster
# messages from node 4 — messages sent before failover, when 4 was still a replica of 0.
#
# Upon processing these stale messages, node 7 incorrectly believes node 4 is still
# following 0, reconfigures itself as a sub-replica of 0, and immediately starts a new
# election when it sees 0 marked as FAILED. Since it is now the only "replica" of 0,
# node 7 wins and becomes an empty primary in the same shard as node 4.
#
# This test verifies that such race condition does not lead to empty primaries or
# duplicate leaders within the same shard. The scenario reflects a realistic replication
# race that can occur whenever replicas connect during overlapping RDB saves or network
# partitions, even without artificial delays.
proc test_blocked_replica_stale_state_race {type} {
    test "Blocked replica mistakenly become sub-replica and gets fixed later - $type" {
        R 3 config set cluster-replica-validity-factor 0
        R 7 config set cluster-replica-validity-factor 0
        R 3 config set cluster-allow-replica-migration yes
        R 7 config set cluster-allow-replica-migration yes

        populate_data
        move_slot_0_from_primary_3_to_primary_0

        # Make sure server 3 and server 7 become a replica of primary 0.
        set R0_id [R 0 CLUSTER MYID]
        wait_for_log_messages -3 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R0_id*"] 0 1000 10
        wait_for_log_messages -7 [list "*Lost my last slot during slot migration. Reconfiguring myself as a replica of $R0_id*"] 0 1000 10

        # Stop primary 0 to start a failover.
        set primary0_pid [stop_primary_0 $type]

        # Wait for the replica to become a primary, and make sure
        # the other primary become a replica.
        set R4_id [R 4 CLUSTER MYID]
        wait_for_log_messages -4 {"*Failover election won: I'm the new primary*"} 0 1000 10
        wait_for_log_messages -3 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R4_id*"] 0 1000 10

        # Notice the ordering here is different from the previous sub-replica test function where
        # the replica 7 becomes a sub-replica first and then reconfigures to follow primary 4.
        # But here replica 7 reconfigures to follow primary 4 first and then mistakenly finds
        # out it's a sub-replica.
        set matched_result [wait_for_log_messages -7 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R4_id*"] 0 1000 10]
        set line_number [lindex $matched_result 1]
        wait_for_log_messages -7 [list "*I'm a sub-replica! Reconfiguring myself as a replica of $R0_id*"] $line_number 1000 10

        # Later replica 7 will start following primary 4 again.
        wait_for_log_messages -7 [list "*Sender $R4_id* and I are in the same shard and I should follow it"] $line_number 1000 10

        # Wait for the cluster to be ok.
        wait_for_cluster_propagation_except_node 0

        # Make sure the key exists and is consistent.
        R 3 readonly
        R 7 readonly
        wait_for_key_consistent {3 4 7} {key_991803 1024 key_977613 10240}

        resume_primary_0 $type $primary0_pid
    }
}

# For this test case, we only do "sigstop" type. Sending shutdown would leave
# the primary 0 in state "Waiting for replicas before shutting down" and meanwhile
# replica 7 is waiting for replication reply from it. This creates a circular
# dependency, and thus replica 4 couldn't get promoted to primary during the
# replica 7 blocking period. Then we can't create the edge case we're trying
# test here.

# This test is currently disabled because it's flaky. If server 7 receives all
# stale PING packets from server 4 (via inbound link) before receiving PONG reply
# from it (via outbound link), the tricky empty primary scenario won't happen,
# and thus this test case won't be applicable.

#start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
#    test_blocked_replica_stale_state_race "sigstop"
#} my_slot_allocation cluster_allocate_replicas ;# start_cluster

proc test_cluster_setslot {type} {
    test "valkey-cli make source node ignores NOREPLICAS error when doing the last CLUSTER SETSLOT - $type" {
        R 3 config set cluster-allow-replica-migration no
        R 7 config set cluster-allow-replica-migration yes

        if {$type == "setslot"} {
            # Make R 7 drop the PING message so that we have a higher
            # chance to trigger the migration from CLUSTER SETSLOT.
            R 7 DEBUG DROP-CLUSTER-PACKET-FILTER 1
        }

        move_slot_0_from_primary_3_to_primary_0

        # Wait for R 3 to report that it is an empty primary (cluster-allow-replica-migration no)
        wait_for_log_messages -3 {"*I am now an empty primary*"} 0 1000 50

        if {$type == "setslot"} {
            R 7 DEBUG DROP-CLUSTER-PACKET-FILTER -1
        }

        # Make sure server 3 lost its replica (server 7) and server 7 becomes a replica of primary 0.
        set addr "[srv 0 host]:[srv 0 port]"
        wait_for_condition 1000 50 {
            [s -3 role] eq {master} &&
            [s -3 connected_slaves] eq 0 &&
            [s -7 role] eq {slave} &&
            [get_my_primary_peer 7] eq $addr
        } else {
            puts "R 3 role: [R 3 role]"
            puts "R 7 role: [R 7 role]"
            fail "Server 3 and 7 role response has not changed"
        }
    }
}

start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test_cluster_setslot "gossip"
} my_slot_allocation cluster_allocate_replicas ;# start_cluster

start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test_cluster_setslot "setslot"
} my_slot_allocation cluster_allocate_replicas ;# start_cluster

start_cluster 3 0 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test "Empty primary will check and delete the dirty slots" {
        R 2 config set cluster-allow-replica-migration no

        # Write a key to slot 0.
        R 2 incr key_977613

        # Move slot 0 from primary 2 to primary 0.
        R 0 cluster bumpepoch
        R 0 cluster setslot 0 node [R 0 cluster myid]

        # Wait for R 2 to report that it is an empty primary (cluster-allow-replica-migration no)
        wait_for_log_messages -2 {"*I am now an empty primary*"} 0 1000 50

        # Make sure primary 0 will delete the dirty slots.
        verify_log_message -2 "*Deleting keys in dirty slot 0*" 0
        assert_equal [R 2 dbsize] 0
    }
} my_slot_allocation cluster_allocate_replicas ;# start_cluster
