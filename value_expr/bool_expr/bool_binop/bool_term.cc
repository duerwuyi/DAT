#include "bool_term.hh"
#include "../../expr.hh"

bool_term::bool_term(prod *p) : bool_binop(p)
{
    op = ((d6() < 4) ? "or" : "and");
    lhs = bool_expr::factory(this);
    rhs = bool_expr::factory(this);
}

bool_term::bool_term(prod *p, bool is_or,
                     shared_ptr<bool_expr> given_lhs,
                     shared_ptr<bool_expr> given_rhs) : bool_binop(p)
{
    op = (is_or ? "or" : "and");
    lhs = given_lhs;
    rhs = given_rhs;
}

void bool_term::out(ostream &out)
{
    
    if (has_equal_expr == false)
    {
        out << "(" << *lhs << ") ";
        indent(out);
        out << op << " (" << *rhs << ")";
    }
    else
    {
        out << *equal_expr;
    }
}


void bool_term::set_component_id(int &id)
{
    bool_binop::set_component_id(id);
    if (has_equal_expr)
        equal_expr->set_component_id(id);
}

bool bool_term::get_component_from_id(int id, shared_ptr<value_expr> &component)
{
    if (has_equal_expr)
        GET_COMPONENT_FROM_ID_CHILD(id, component, equal_expr);
    return bool_binop::get_component_from_id(id, component);
}

bool bool_term::set_component_from_id(int id, shared_ptr<value_expr> component)
{
    if (has_equal_expr)
    {
        auto bool_value = dynamic_pointer_cast<bool_expr>(component);
        if (bool_value)
            SET_COMPONENT_FROM_ID_CHILD(id, bool_value, equal_expr);
        else
        {
            if (equal_expr->set_component_from_id(id, bool_value)) 
                return true;
        }
    }
    return bool_binop::set_component_from_id(id, component);
}