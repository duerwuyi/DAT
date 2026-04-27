#pragma once

#include "../value_expr.hh"


struct bool_expr : value_expr
{
    virtual ~bool_expr() {}
    bool_expr(prod *p) : value_expr(p) { type = scope->schema->booltype; }
    static shared_ptr<bool_expr> factory(prod *p);

    shared_ptr<bool_expr> eq_bool_expr;
};