#include "in_query.hh"
#include "grammar.hh"

extern int use_group;    // 0->no group, 1->use group, 2->to_be_define
extern int in_in_clause; // 0-> not in "in" clause, 1-> in "in" clause (cannot use limit)

in_query::in_query(prod *p) : bool_expr(p), myscope(scope)
{
    myscope.tables = scope->tables;
    scope = &myscope;

    lhs = value_expr::factory(this);

    auto tmp_in_state = in_in_clause;
    in_in_clause = 1;
    auto tmp_use_group = use_group;
    use_group = 0;

    // clickhouse does not support correlated subqueries.
    // it use seperated my scope, do not need to restore the refs
    // if (schema::target_dbms == "clickhouse")// bug #37438 in ss
    //     scope->refs.clear(); // dont use the ref of parent select
    if (this->scope->schema->target_ddbms == "ss")// bug #37438 in ss
        scope->refs.clear(); // dont use the ref of parent select

    vector<sqltype *> pointed_type;
    pointed_type.push_back(lhs->type);
    if (d6() < 4)
        in_subquery = make_shared<unioned_query>(this, scope, false, &pointed_type);
    else
        in_subquery = make_shared<query_spec>(this, scope, false, &pointed_type);

    use_group = tmp_use_group;
    in_in_clause = tmp_in_state;
}

in_query::in_query(prod *p, shared_ptr<value_expr> expr, shared_ptr<prod> subquery)
    : bool_expr(p), myscope(scope)
{
    myscope.tables = scope->tables;
    scope = &myscope;

    lhs = expr;

    in_subquery = subquery;
}

void in_query::out(ostream &out)
{
    if (scope->schema->target_ddbms == "clickhouse" && d6() < 4){
        out << "(" << *lhs << ") global in (" << *in_subquery << ")";
    }

    if (is_transformed && schema::target_dbms != "clickhouse")
        out << *eq_expr;
    else
        out << "(" << *lhs << ") in (" << *in_subquery << ")";
}

void in_query::accept(prod_visitor *v)
{
    v->visit(this);
    lhs->accept(v);
    in_subquery->accept(v);
}

void in_query::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    lhs->set_component_id(id);

    auto tmp1 = dynamic_pointer_cast<query_spec>(in_subquery);
    if (tmp1)
    {
        tmp1->search->set_component_id(id);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(in_subquery);
    if (tmp2)
    {
        tmp2->lhs->search->set_component_id(id);
        tmp2->rhs->search->set_component_id(id);
    }
}

bool in_query::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    auto tmp1 = dynamic_pointer_cast<query_spec>(in_subquery);
    if (tmp1)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp1->search);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(in_subquery);
    if (tmp2)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->lhs->search);
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->rhs->search);
    }

    GET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    return bool_expr::get_component_from_id(id, component);
}

bool in_query::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    auto bool_value = dynamic_pointer_cast<bool_expr>(component);
    auto tmp1 = dynamic_pointer_cast<query_spec>(in_subquery);
    auto tmp2 = dynamic_pointer_cast<unioned_query>(in_subquery);
    if (bool_value)
    {
        if (tmp1)
        {
            SET_COMPONENT_FROM_ID_CHILD(id, bool_value, tmp1->search->search);
        }
        if (tmp2)
        {
            SET_COMPONENT_FROM_ID_CHILD(id, bool_value, tmp2->lhs->search->search);
            SET_COMPONENT_FROM_ID_CHILD(id, bool_value, tmp2->rhs->search->search);
        }
    }
    else
    {
        if (tmp1)
        {
            if (tmp1->search->set_component_from_id(id, component))
                return true;
        }
        if (tmp2)
        {
            if (tmp2->lhs->search->set_component_from_id(id, component))
                return true;
            if (tmp2->rhs->search->set_component_from_id(id, component))
                return true;
        }
    }

    SET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    return bool_expr::set_component_from_id(id, component);
}