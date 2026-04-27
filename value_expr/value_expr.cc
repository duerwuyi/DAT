#include "value_expr.hh"
#include "expr.hh"

extern int in_in_clause; // 0-> not in "in" clause, 1-> in "in" clause (cannot use limit)

shared_ptr<value_expr> value_expr::factory(prod *p, sqltype *type_constraint,
                                           vector<shared_ptr<named_relation>> *prefer_refs)
{
    try
    {
        if (p->scope->schema->booltype == type_constraint)
            return bool_expr::factory(p);

        if (type_constraint == NULL && d12() == 1)
            return bool_expr::factory(p);

        if (p->level < d6() && p->level < max_level)
        {
            auto choice = d42();
            if ((choice <= 2) && window_function::allowed(p) && schema::target_ddbms != "vitess") //bug 19530
                return make_shared<window_function>(p, type_constraint);
            if (choice == 3)
                return make_shared<coalesce>(p, type_constraint);
            if (choice == 4)
                return make_shared<nullif>(p, type_constraint);
            if (choice <= 11 && schema::target_ddbms != "vitess")
                return make_shared<funcall>(p, type_constraint);
            if (choice <= 16)
                return make_shared<case_expr>(p, type_constraint);
            if (choice <= 21)
                return make_shared<binop_expr>(p, type_constraint);
        }
        auto choice = d42();
        if (in_in_clause == 0 && choice <= 12 && schema::target_ddbms != "vitess")
            return make_shared<atomic_subselect>(p, type_constraint);
        if (p->scope->refs.size() && choice <= 40)
            return make_shared<column_reference>(p, type_constraint, prefer_refs);
        return make_shared<const_expr>(p, type_constraint);
    }
    catch (runtime_error &e)
    {
    }
    p->retry();
    return factory(p, type_constraint);
}

void value_expr::set_component_id(int &id)
{
    assert(is_transformed == false);
    component_id = id;
    id++;
};