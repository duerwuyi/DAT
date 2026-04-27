// #ifndef TBASE_SQL_TESTER_HH
// #define TBASE_SQL_TESTER_HH

// #include "tbase.hh"

// struct tbase_sql_dut : tbase_dut
// {
//     virtual void reset_to_backup(void) override;
//     tbase_sql_dut(string db,string ip, unsigned int port)
//     : tbase_dut(db, ip, port) {}
// };

// //only for storing the record file
// struct tbase_distributor : distributor
// {
//     shared_ptr<tbase_context> context;
//     virtual void distribute_all(void) {};
//     tbase_distributor(shared_ptr<tbase_context> context);
//     virtual string distribute_one(shared_ptr<prod> gen, int* affect_num) override;
//     virtual void clear_record_file(void) override;
// };

// struct tbase_distribute_action : tbase_action{
//     shared_ptr<create_table_stmt> create_table;
//     virtual bool run(Context& ctx, int* affect_num);
//     virtual action* clone() const override {
//         return new tbase_distribute_action(*this);
//     }
// };

// struct distribute_by_shard : tbase_distribute_action
// {
//     column* dist_column;
//     virtual void random_fill(Context& ctx) override;
//     virtual action* clone() const override {
//         return new distribute_by_shard(*this);
//     }
// };

// struct distribute_by_replication : tbase_distribute_action
// {
//     virtual void random_fill(Context& ctx) override;
//     virtual action* clone() const override {
//         return new distribute_by_replication(*this);
//     }
// };

// #endif
