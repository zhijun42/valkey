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

proc test_migrated_replica {type} {
    test "Migrated replica reports zero repl offset and rank, and fails to win election - $type" {
        # Validate that shard 3's primary and replica can convert to replicas after
        # they lose the last slot.
        R 3 config set cluster-replica-validity-factor 0
        R 7 config set cluster-replica-validity-factor 0
        R 3 config set cluster-allow-replica-migration yes
        R 7 config set cluster-allow-replica-migration yes

        set R0_id [R 0 CLUSTER MYID]
        set R3_id [R 3 CLUSTER MYID]
        set R4_id [R 4 CLUSTER MYID]

        populate_data
        move_slot_0_from_primary_3_to_primary_0

        # Ensure slot migration does happen
        wait_for_log_messages -3 [list "*Slot 0 is no longer being migrated to node $R0_id*"] 0 1000 10
        wait_for_log_messages -3 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R0_id*"] 0 1000 10
        wait_for_log_messages -7 [list "*Lost my last slot during slot migration. Reconfiguring myself as a replica of $R0_id*"] 0 1000 10

        # At this point primary 0 should have 3 replicas (2 newly added).
#        wait_for_condition 1000 50 {
#            [RI 0 connected_slaves] eq 0
#        } else {
#            fail "R3 and R7 didn't become replica after losing all slots"
#        }

        # Stop primary 0 to start a failover.
        set primary0_pid [stop_primary_0 $type]

        # Wait for the replica to become a primary, and make sure
        # the other primary become a replica.

        # Wait for the replica R4 to become new primary, and make sure
        # replicas R3 and R7 are following it.
        wait_for_log_messages -3 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R4_id*"] 0 1000 10
        wait_for_log_messages -7 [list "*Configuration change detected. Reconfiguring myself as a replica of node $R4_id*"] 0 1000 10

        wait_for_condition 1000 50 {
            [s -4 role] eq {master} &&
            [s -3 role] eq {slave} &&
            [s -7 role] eq {slave}
        } else {
            puts "s -4 role: [s -4 role]"
            puts "s -3 role: [s -3 role]"
            puts "s -7 role: [s -7 role]"
            fail "Failover does not happen"
        }

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

        set slots [R 7 cluster slots]
        puts "R7 slots $slots"
        set slots [R 3 cluster slots]
        puts "R3 slots $slots"
        set slots [R 4 cluster slots]
        puts "R4 slots $slots"
        set info [R 7 INFO]
        puts "R7 info $info"

        set shards [R 7 cluster shards]
        puts "R7 shards $shards"
        set shards [R 3 cluster shards]
        puts "R3 shards $shards"
        set shards [R 4 cluster shards]
        puts "R4 shards $shards"

        after 20000

        puts "~~20s~~~"
        set shards [R 7 cluster shards]
        puts "R7 shards $shards"
        set shards [R 3 cluster shards]
        puts "R3 shards $shards"
        set shards [R 4 cluster shards]
        puts "R4 shards $shards"

        # Make sure the key exists and is consistent.
        R 3 readonly
        R 7 readonly
        wait_for_condition 1000 50 {
            [R 3 get key_991803] == 1024 && [R 3 get key_977613] == 10240 &&
            [R 4 get key_991803] == 1024 && [R 4 get key_977613] == 10240 &&
            [R 7 get key_991803] == 1024 && [R 7 get key_977613] == 10240
        } else {
            puts "R 3: [R 3 keys *]"
            puts "R 4: [R 4 keys *]"
            puts "R 7: [R 7 keys *]"
            fail "Key not consistent"
        }

        resume_primary_0 $type $primary0_pid
    }
} ;# proc

#start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
#    test_migrated_replica "shutdown"
#} my_slot_allocation cluster_allocate_replicas ;# start_cluster

start_cluster 4 4 {tags {external:skip cluster} overrides {cluster-node-timeout 1000 cluster-migration-barrier 999}} {
    test_migrated_replica "sigstop"
} my_slot_allocation cluster_allocate_replicas ;# start_cluster
