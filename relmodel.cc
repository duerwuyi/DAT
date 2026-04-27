#include "relmodel.hh"

static int relation_id_counter = 0; 

int unique_relation_id() {
    relation_id_counter++;
    // cout<<"unique_relation_id: "<<relation_id_counter<<endl;
    return relation_id_counter;
}

void reset_relation_id_counter() {
    relation_id_counter = 0;
}

map<string, sqltype*> sqltype::typemap;

sqltype * sqltype::get(string n)
{
  if (typemap.count(n))
    return typemap[n];
  else
    return typemap[n] = new sqltype(n);
}

void sqltype::clear_cache()
{
  for (auto& it : typemap)
    delete it.second;
  typemap.clear();
}

bool sqltype::consistent(struct sqltype *rvalue)
{
  return this == rvalue;
}


void TableVersions::insert(shared_ptr<table> table_ptr) {
  if(table_ptr -> get_id_from_name() == -1) {
      return;
  }
  int id = (table_ptr -> get_id_from_name()) % total_groups;
  table_id_with_versions.insert(make_pair(id, table_ptr));
}

std::shared_ptr<table> TableVersions::random_remove(int id) {
  // Case 1: specific id provided
  if (id != -1) {
      auto range = table_id_with_versions.equal_range(id);
      size_t count = std::distance(range.first, range.second);

      if (count <= 1) return nullptr;

      auto it = random_pick(range); // pick random iterator in range
      std::shared_ptr<table> result = it->second;
      table_id_with_versions.erase(it);
      return result;
  }

  // Case 2: id == -1, find all ids with >= 2 versions
  std::unordered_map<int, std::vector<std::multimap<int, std::shared_ptr<table>>::iterator>> id_to_iters;

  for (auto it = table_id_with_versions.begin(); it != table_id_with_versions.end(); ++it) {
      id_to_iters[it->first].push_back(it);
  }

  std::vector<int> candidate_ids;
  for (const auto& [key, vec] : id_to_iters) {
      if (vec.size() > 1) candidate_ids.push_back(key);
  }

  if (candidate_ids.empty()) return nullptr;

  int selected_id = random_pick(candidate_ids);  // pick random id
  auto& vec = id_to_iters[selected_id];
  auto it = random_pick(vec);                    // pick random iterator in that id group

  std::shared_ptr<table> result = it->second;
  table_id_with_versions.erase(it);
  return result;
}

std::vector<std::shared_ptr<table>> TableVersions::clear_all_but_one_per_id() {
  std::vector<std::shared_ptr<table>> removed_tables;

  // Step 1:  id   
  std::unordered_map<int, std::vector<std::multimap<int, std::shared_ptr<table>>::iterator>> id_to_iters;

  for (auto it = table_id_with_versions.begin(); it != table_id_with_versions.end(); ++it) {
      id_to_iters[it->first].push_back(it);
  }

  // Step 2:  id 
  for (auto& [id, vec] : id_to_iters) {
      if (vec.size() <= 1) continue;

      // 
      auto keep_it = random_pick(vec);

      for (auto it : vec) {
          if (it == keep_it) continue;
          removed_tables.push_back(it->second);
          table_id_with_versions.erase(it);
      }
  }

  return removed_tables;
}

std::string TableVersions::allocate_next_table_name_for_id(int value) {
  int logic_id = value % total_groups;

  //  id 
  std::set<int> used_table_ids;
  auto range = table_id_with_versions.equal_range(logic_id);
  for (auto it = range.first; it != range.second; ++it) {
      auto tbl_ptr = it->second;
      int id = tbl_ptr->get_id_from_name();
      used_table_ids.insert(id);
  }

  // 
  int version = 0;
  while (true) {
      int candidate_id = total_groups * version + logic_id;
      if (!used_table_ids.count(candidate_id)) {
          return "t" + std::to_string(candidate_id);
      }
      ++version;
  }
}
