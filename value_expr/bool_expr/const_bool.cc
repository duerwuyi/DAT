#include "const_bool.hh"
#include "../expr.hh"

const_bool::const_bool(prod *p) : bool_expr(p)
{
    auto choice = d6();
    if (choice <= 2)
        op = scope->schema->true_literal;
    else if (choice <= 4)
        op = scope->schema->false_literal;
    else
        op = scope->schema->null_literal;
}

// 1: true_literal
// 0: false_literal
// -1: null_literal
const_bool::const_bool(prod *p, int is_true) : bool_expr(p)
{
    if (is_true > 0)
        op = scope->schema->true_literal;
    else if (is_true == 0)
        op = scope->schema->false_literal;
    else
        op = scope->schema->null_literal;
}

void const_bool::out(std::ostream &out)
{
    if (!is_transformed) {
        if (scope->schema->target_dbms == "postgres")
            out << op << "::" << scope->schema->booltype->name;
        else
            out << op;
    }
    else
        out << *eq_expr;
}
