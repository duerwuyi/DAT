#include "exists_predicate.hh"
#include "grammar.hh"

exists_predicate::exists_predicate(prod *p) : bool_expr(p), myscope(scope)
{
    myscope.tables = scope->tables;
    scope = &myscope;

    // clickhouse does not support correlated subqueries.
    // it use seperated my scope, do not need to restore the refs
    // if (schema::target_dbms == "clickhouse")
    //     scope->refs.clear();
    
    // disable order and limit to make sure that exists clause can be accurately transformed to in clause
    if (d6() < 4)
    {
        auto tmp = make_shared<unioned_query>(this, scope);
        tmp->lhs->has_order = false;
        tmp->lhs->has_limit = false;
        tmp->rhs->has_order = false;
        tmp->rhs->has_limit = false;
        subquery = tmp;
    }
    else
    {
        auto tmp = make_shared<query_spec>(this, scope);
        tmp->has_order = false; // disable order
        tmp->has_limit = false; // disable limit
        subquery = tmp;
    }
}

exists_predicate::exists_predicate(prod *p, shared_ptr<prod> subquery)
    : bool_expr(p)
{
    this->subquery = subquery;
}

void exists_predicate::accept(prod_visitor *v)
{
    v->visit(this);
    subquery->accept(v);
}

void exists_predicate::out(std::ostream &out)
{
    
    if (is_transformed)
    {
        out << *eq_exer;
        return;
    }

    out << "exists (";
    indent(out);
    out << *subquery << ")";
}

void exists_predicate::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    auto tmp1 = dynamic_pointer_cast<query_spec>(subquery);
    if (tmp1)
    {
        tmp1->search->set_component_id(id);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(subquery);
    if (tmp2)
    {
        tmp2->lhs->search->set_component_id(id);
        tmp2->rhs->search->set_component_id(id);
    }
}

bool exists_predicate::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    auto tmp1 = dynamic_pointer_cast<query_spec>(subquery);
    if (tmp1)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp1->search);
    }
    auto tmp2 = dynamic_pointer_cast<unioned_query>(subquery);
    if (tmp2)
    {
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->lhs->search);
        GET_COMPONENT_FROM_ID_CHILD(id, component, tmp2->rhs->search);
    }
    return bool_expr::get_component_from_id(id, component);
}

bool exists_predicate::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    auto bool_value = dynamic_pointer_cast<bool_expr>(component);
    auto tmp1 = dynamic_pointer_cast<query_spec>(subquery);
    auto tmp2 = dynamic_pointer_cast<unioned_query>(subquery);
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
    return bool_expr::set_component_from_id(id, component);
}
