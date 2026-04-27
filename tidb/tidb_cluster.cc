#include "tidb_cluster.hh"
#include <chrono>
#include <thread>
#define TIDB_SAVING_DIR "tidb/saved/"
#define TIDB_CLUSTER_ACTIONS "tidb_actions.log"
#define TIDB_DIST_RECORD_FILE "tidb_setup.sql"

tidb_cluster_fuzzer::tidb_cluster_fuzzer(shared_ptr<tidb_context> ctx)
: cluster_fuzzer(ctx, TIDB_SAVING_DIR + string(TIDB_CLUSTER_ACTIONS))
{
    base_context = ctx;
    auto context = dynamic_pointer_cast<tidb_context>(base_context);
    context -> logfile = TIDB_SAVING_DIR + string(TIDB_CLUSTER_ACTIONS);
    // context -> master_dut = make_shared<tidb_dut>(db, ip, port);
}

void tidb_cluster_fuzzer::reset_init(){
    auto context = dynamic_pointer_cast<tidb_context>(base_context);
    context -> master_dut -> reset();
}

tidb_partition_table::tidb_partition_table(shared_ptr<table> victim_table, prod *parent,shared_ptr<column> d_key,string strategy
    ,int shard_num,set<int64_t>* boundary)
:create_dds(victim_table,parent){
    if(victim_table == nullptr){
        throw runtime_error("Tidb needs a victim table to pratition.");
    }else{
        victim = victim_table;
    }
    if(boundary != nullptr){
        this-> boundary = *boundary;
    }

    if(d_key == nullptr){
        column col = random_pick(victim -> columns());
        this -> dkey = make_shared<column>(col);
    }else{
        this -> dkey = d_key;
    }

    if(shard_num == -1){
        if(d6() < 2){
            this -> shard_num = dx(4) + 1;
        }
        else{
            this -> shard_num = dx(20) + 5;
        }
    }
    else this -> shard_num = shard_num;

    if(strategy == ""){
        vector<string> strategy_list = {};
        if(this->dkey->type == parent->scope->schema->inttype){
            strategy_list.push_back("RANGE");
            strategy_list.push_back("HASH");
            strategy_list.push_back("KEY");
        }else if(this -> dkey->type == parent->scope->schema->realtype){
            strategy_list.push_back("KEY");
        }else if(this -> dkey->type == parent->scope->schema->datetype){
            // strategy_list.push_back("RANGE");
            strategy_list.push_back("KEY");
        }else if(this -> dkey->type == parent->scope->schema->texttype){
            strategy_list.push_back("KEY");
        }
        this->method = random_pick(strategy_list);
    }else{
        this->method = strategy;
    }
}

void tidb_partition_table::out(std::ostream &out){
    if(do_nothing){
        return;
    }
    out<<"PARTITION BY "<<method<<" ("<<dkey->name<<")"<<endl;
    if(method == "RANGE"){
        out<<"("<<endl;
        int64_t lower = -2147483648;
        if(d6()==1){
            lower = -150 + d100();
        }
        int64_t upper = 2147483647;
        if(d6()==1){
            upper = 50 + d100();
        }
        size_t need = (shard_num > 1) ? shard_num : 2;
        int64_t domain = upper - lower + 1;

        while (boundary.size() < need) boundary.insert(rand_int64(lower, upper));
        size_t i = 0;
        for (auto b : boundary) {
            out<<"PARTITION p"<<i<<" VALUES LESS THAN ("<<b<<"),";
            i++;
        }
        out<<"PARTITION p"<<i<<" VALUES LESS THAN MAXVALUE";
        out<<")";
    }else{
        out<<"PARTITIONS " << shard_num;
    }
    
}

tidb_shard_rowid_table::tidb_shard_rowid_table(shared_ptr<table> victim_table, prod *parent,
                                               int shard_row_id_bits, int pre_split_regions)
    : create_dds(victim_table, parent) {
    if (victim_table == nullptr) {
        throw runtime_error("Tidb needs a victim table to set SHARD_ROW_ID_BITS / PRE_SPLIT_REGIONS.");
    } else {
        victim = victim_table;
    }

    // SHARD_ROW_ID_BITS:
    // - 0 is effectively disabled; we usually pick a small positive number to make the layout change observable.
    // - keep small to avoid creating too many regions for fuzzing.
    if (shard_row_id_bits == -1) {
        // Bias towards enabled.
        this->shard_row_id_bits = (d6() <= 4) ? (dx(5)) : 0; // 1..5 or 0
    } else {
        this->shard_row_id_bits = shard_row_id_bits;
    }

    // PRE_SPLIT_REGIONS:
    // TiDB will pre-split into 2^N regions. Must be <= SHARD_ROW_ID_BITS when SHARD_ROW_ID_BITS > 0.
    if (pre_split_regions == -1) {
        if (this->shard_row_id_bits <= 0) {
            this->pre_split_regions = 0;
        } else {
            // Keep <= shard_row_id_bits and small (0..min(3, bits)).
            int max_pre = std::min(3, this->shard_row_id_bits);
            this->pre_split_regions = dx(max_pre + 1); // 0..max_pre
        }
    } else {
        this->pre_split_regions = pre_split_regions;
    }

    // If both are effectively no-op, allow caller to skip output.
    if (this->shard_row_id_bits == 0 && this->pre_split_regions == 0) {
        this->do_nothing = true;
    }
}

void tidb_shard_rowid_table::out(std::ostream &out) {
    if (do_nothing) {
        return;
    }
    // These are table options (placed after column list and before PARTITION BY ...).
    // Use spaces (not commas) to be consistent with MySQL/TiDB table option grammar.
    out << "SHARD_ROW_ID_BITS = " << shard_row_id_bits;
    if (pre_split_regions >= 0) {
        out << " PRE_SPLIT_REGIONS = " << pre_split_regions;
    }
    out << std::endl;
}

tidb_set_tiflash_replica::tidb_set_tiflash_replica(shared_ptr<table> victim_table, prod *parent, int replica_count)
: create_dds(victim_table, parent) {
    if (victim_table == nullptr) {
        throw runtime_error("Tidb needs a victim table to set TiFlash replica.");
    }
    victim = victim_table;

    this->replica_count = replica_count;
    if (this->replica_count < 0) this->replica_count = 1;
}

void tidb_set_tiflash_replica::out(std::ostream &out) {
    if (do_nothing) return;
    out << "ALTER TABLE " << victim->name << " SET TIFLASH REPLICA " << replica_count;
}

string tidb_distributor::distribute_one(shared_ptr<prod> gen, int* affect_num, int i){
    auto create_table = dynamic_pointer_cast<create_table_stmt>(gen);
    if(create_table == nullptr){
        throw runtime_error("not create_table_stmt");
    }
    int groupid = i % total_groups;
    int id_in_group = i / total_groups;
    shared_ptr<create_dds> dds = nullptr;
    shared_ptr<create_dds> dds1 = nullptr;
    shared_ptr<create_dds> dds2 = nullptr;

    if(id_in_group == 0){
        ostringstream s;
        gen->out(s);
        auto sql = s.str() + ";";
        ctx -> master_dut -> test(sql);
        return sql;
    }else if(id_in_group == 1){
        dds = make_shared<tidb_partition_table>(create_table->created_table,gen.get());
    }else if(id_in_group == 2){
        if(groupid != 0){
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("");

            dds = make_shared<tidb_partition_table>(
                create_table->created_table,
                gen.get(),
                this -> used_for_binding ->dkey,
                this -> used_for_binding ->method,
                this -> used_for_binding ->shard_num,
                &(this -> used_for_binding ->boundary)
            );
            this ->binding_tables.push_back(create_table->created_table->name);
        }else{
            vector<sqltype *> enable_type;
            enable_type.push_back(gen->scope->schema->inttype);
            enable_type.push_back(gen->scope->schema->texttype);
            enable_type.push_back(gen->scope->schema->realtype);
            enable_type.push_back(gen->scope->schema->datetype);
            auto type = random_pick<>(enable_type);

            colocate_column = make_shared<column>(COLOCATE_NAME, type);
            create_table->created_table->columns().push_back(*colocate_column);
            create_table->constraints.push_back("");
            
            this -> used_for_binding = make_shared<tidb_partition_table>(
                create_table->created_table,
                gen.get(),
                colocate_column
            );
            dds = this -> used_for_binding;
            this ->binding_tables.push_back(create_table->created_table->name);
        }
    }
    else{
        if(d6()<3)
            dds = make_shared<tidb_shard_rowid_table>(create_table->created_table,gen.get());
        if(d6()<3)
            dds1 = make_shared<tidb_partition_table>(create_table->created_table,gen.get());
        if(d6()<3)
            dds2 = make_shared<tidb_set_tiflash_replica>(create_table->created_table, gen.get(), 1);
    }

    ostringstream s,s0,s1,s2;
    gen->out(s);
    if(dds){
        dds->out(s0);
    }
    if(dds1){
        dds1->out(s1);
    }
    if(dds2){
        dds2->out(s2);
    }
    auto sql4single = s.str() + ";";
    auto sql = s.str()+s0.str()+s1.str()+";";
    auto sql2 = s2.str();
    ctx -> master_dut -> test(sql);
    if(dds2){
        ctx -> master_dut -> test(sql2);
    }
    return sql4single;
}

// void tidb_distributor::wait_tiflash_replicas_ready(int max_rounds, int sleep_ms) {
//     if (max_rounds <= 0) max_rounds = 1;
//     if (sleep_ms < 0) sleep_ms = 0;

//     // We avoid relying on a result-set API here by using a probe query that
//     // deterministically errors while any replica is not ready.
//     // Ready condition: no rows where AVAILABLE < 1 OR PROGRESS < 1.
//     const string probe =
//         "SELECT CASE WHEN EXISTS("
//         "  SELECT 1 FROM information_schema.tiflash_replica "
//         "  WHERE AVAILABLE < 1 OR PROGRESS < 1"
//         ") THEN JSON_EXTRACT('not json', '$.a') ELSE 1 END";

//     for (int round = 0; round < max_rounds; round++) {
//         try {
//             ctx->master_dut->test(probe + ";");
//             return;
//         } catch (const std::exception &) {
//             if (sleep_ms > 0) {
//                 std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
//             }
//         }
//     }
//     throw runtime_error("Timeout waiting for TiFlash replicas to become ready");
// }

void wait_tiflash_replicas_ready(shared_ptr<tidb_context> ctx,int max_rounds, int sleep_ms) {
    if(!ctx) throw runtime_error("ctx null in wait_tiflash_replicas_ready()");
    if (max_rounds <= 0) max_rounds = 1;
    if (sleep_ms < 0) sleep_ms = 0;

    // Return two scalars:
    //   total:     total number of rows in information_schema.tiflash_replica
    //   not_ready: number of rows where AVAILABLE<1 or PROGRESS<1
    //
    // We avoid EXISTS/subquery here due to a TiDB internal error observed on tiflash_replica.
    const std::string sql =
        "SELECT "
        "  COUNT(*) AS total, "
        "  SUM(CASE WHEN AVAILABLE < 1 OR PROGRESS < 1 THEN 1 ELSE 0 END) AS not_ready "
        "FROM information_schema.tiflash_replica";

    for (int round = 0; round < max_rounds; round++) {
        try {
            std::vector<std::vector<std::string>> out;
            ctx->master_dut->test(sql + ";", &out);

            // Expect one row, two columns.
            if (!out.empty() && out[0].size() >= 2) {
                long long total = 0;
                long long not_ready = 0;

                try {
                    total = std::stoll(out[0][0]);
                    not_ready = std::stoll(out[0][1]);
                } catch (...) {
                    // If parsing fails, treat as not ready and retry.
                    total = 0;
                    not_ready = 1;
                }

                // If there are no tiflash_replica rows at all, it usually means no replicas configured yet.
                // To avoid a false "ready" immediately after SET TIFLASH REPLICA (metadata propagation),
                // do one extra retry when (round==0 && total==0 && not_ready==0).
                if (not_ready == 0) {
                    // if (!(round == 0 && total == 0))
                        return;
                }
            }
        } catch (const std::exception &) {
            // ignore and retry
        }

        if (sleep_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        }
    }

    throw std::runtime_error("Timeout waiting for TiFlash replicas to become ready");
}

void tidb_distributor::clear_record_file(void){
    //clear the record file string(CITUS_SAVING_DIR) + string(CITUS_DIST_RECORD_FILE)
    std::ofstream file(string(TIDB_SAVING_DIR) + string(TIDB_DIST_RECORD_FILE), std::ios::trunc);
    file.close();
}