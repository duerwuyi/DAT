#include "between_op.hh"
#include "bool_binop/comparison_op.hh"
#include "bool_binop/bool_term.hh"

between_op::between_op(prod *p) : bool_expr(p)
{
    mhs = value_expr::factory(this, scope->schema->inttype);
    lhs = value_expr::factory(this, scope->schema->inttype);
    rhs = value_expr::factory(this, scope->schema->inttype);
}

void between_op::accept(prod_visitor *v)
{
    v->visit(this);
    mhs->accept(v);
    lhs->accept(v);
    rhs->accept(v);
}

void between_op::set_component_id(int &id)
{
    bool_expr::set_component_id(id);
    lhs->set_component_id(id);
    mhs->set_component_id(id);
    rhs->set_component_id(id);
}

void between_op::out(ostream &o)
{
    // OUTPUT_EQ_BOOL_EXPR(o);
    
    if (use_eq_expr == false)
        o << "(" << *mhs << ") between (" << *lhs << ") and (" << *rhs << ")";
    else
        o << *eq_expr;
}

bool between_op::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    GET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    GET_COMPONENT_FROM_ID_CHILD(id, component, mhs);
    GET_COMPONENT_FROM_ID_CHILD(id, component, rhs);
    return bool_expr::get_component_from_id(id, component);
}

bool between_op::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    SET_COMPONENT_FROM_ID_CHILD(id, component, lhs);
    SET_COMPONENT_FROM_ID_CHILD(id, component, mhs);
    SET_COMPONENT_FROM_ID_CHILD(id, component, rhs);
    return bool_expr::set_component_from_id(id, component);
}