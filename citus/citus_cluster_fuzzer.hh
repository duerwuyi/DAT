#ifndef CITUS_CLUSTER_FUZZER_HH
#define CITUS_CLUSTER_FUZZER_HH
#include "../tester/cluster_fuzzer.hh"
#include "citus.hh"

void read_citus_worker_config_from_file(std::vector<citus_worker_connection>& workers, std::string db);

struct citus_cluster_fuzzer : cluster_fuzzer{

    virtual void reset_init();
    // virtual bool out(std::ostream &out);
    // virtual bool check_constraint();
    // virtual void fulfil_constraint();
    citus_cluster_fuzzer(shared_ptr<citus_context> ctx);
};


// struct add_worker_node : citus_action{
//     string test_ip;
//     unsigned int test_port;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     add_worker_node(citus_worker_connection& c);
//     add_worker_node() {};
//     virtual action* clone() const override {
//         return new add_worker_node(*this);
//     }
// };

// struct remove_worker_node : citus_action{
//     string test_ip;
//     unsigned int test_port;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     remove_worker_node(citus_worker_connection& c);
//     remove_worker_node() {};
//     virtual action* clone() const override {
//         return new remove_worker_node(*this);
//     }
// };

// struct add_secondary_node : citus_action{
//     string test_ip;
//     unsigned int test_port;
//     string primary_ip;
//     unsigned int  primary_port;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     add_secondary_node() {};
//     virtual action* clone() const override {
//         return new add_secondary_node(*this);
//     }
// };


// struct drop_worker_database : citus_action{
//     string test_ip;
//     unsigned int test_port;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     drop_worker_database() {};
//     virtual action* clone() const override {
//         return new drop_worker_database(*this);
//     }
// };

// struct create_worker_database : citus_action{
//     string sql1;
//     string sql2;
//     string test_ip;
//     unsigned int test_port;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     create_worker_database() {};
//     virtual action* clone() const override {
//         return new create_worker_database(*this);
//     }
// };

// struct recreate_worker_database : citus_action{
//     string sql1;
//     string sql2;
//     string sql3;
//     string test_ip;
//     unsigned int test_port;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     recreate_worker_database() {};
//     virtual action* clone() const override {
//         return new recreate_worker_database(*this);
//     }
// };

// struct citus_get_active_worker_nodes : citus_action{
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     citus_get_active_worker_nodes() {};
//     virtual action* clone() const override {
//         return new citus_get_active_worker_nodes(*this);
//     }
// };

// struct citus_check_cluster_node_health : citus_action{
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     citus_check_cluster_node_health() {};
//     virtual action* clone() const override {
//         return new citus_check_cluster_node_health(*this);
//     }
// };

// struct citus_set_coordinator_host : citus_action{
//     string worker_test_ip_to_be_coordinator;
//     unsigned int worker_test_port_to_be_coordinator;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     citus_set_coordinator_host() {};
//     virtual action* clone() const override {
//         return new citus_set_coordinator_host(*this);
//     }
// };

// struct citus_activate_node : citus_action{
//     string test_ip;
//     unsigned int test_port;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     citus_activate_node() {};
//     virtual action* clone() const override {
//         return new citus_activate_node(*this);
//     }
// };

// struct citus_disable_node : citus_action{
//     string test_ip;
//     unsigned int test_port;
//     virtual void random_fill(Context& ctx) override;
//     virtual bool run(Context& ctx) override;
//     citus_disable_node() {};
//     virtual action* clone() const override {
//         return new citus_disable_node(*this);
//     }
// };

#endif
