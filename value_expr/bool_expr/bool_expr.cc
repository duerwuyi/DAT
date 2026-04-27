#include "bool_expr.hh"
#include "../expr.hh"

shared_ptr<bool_expr> bool_expr::factory(prod *p)
{
    try
    {
        if (p->level > d20() || p->level >= max_level)
            return make_shared<const_bool>(p);

        auto choose = d42();
        if(schema::target_ddbms == "vitess") choose = dx(35); //bug 19544
        if (choose <= 13)
            return make_shared<comparison_op>(p);
        else if (choose <= 26)
            return make_shared<bool_term>(p);
        else if (choose <= 28)
            return make_shared<not_expr>(p);
        else if (choose <= 30)
            return make_shared<null_predicate>(p);
        else if (choose <= 32)
            return make_shared<const_bool>(p);
        else if (choose <= 34)
            return make_shared<between_op>(p);
        else if (choose <= 36)
            return make_shared<like_op>(p);
        else if (choose <= 38 && schema::target_ddbms != "vitess")
            return make_shared<in_query>(p);
        else if (choose <= 40 && schema::target_ddbms != "vitess")
            return make_shared<comp_subquery>(p);
        else
            return make_shared<exists_predicate>(p);
        //     return make_shared<distinct_pred>(q);
    }
    catch (runtime_error &e)
    {
    }
    p->retry();
    return factory(p);
}
