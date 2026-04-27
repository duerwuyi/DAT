AC_INIT(SQLsmith, 1.2.1, seltenreich@gmx.de, sqlsmith, https://github.com/anse1/sqlsmith/)
AC_LANG(C++)

AM_INIT_AUTOMAKE(-Wall -Werror foreign)
AC_PROG_CXX
AC_PROG_RANLIB
AX_CXX_COMPILE_STDCXX_17(noext,mandatory)

AX_LIB_POSTGRESQL()
PKG_CHECK_MODULES(LIBPQXX, libpqxx >= 4.0)

# PKG_CHECK_MODULES(MONETDB_MAPI, monetdb-mapi >= 11.23.0,
# 		[AC_DEFINE([HAVE_MONETDB], [1], [define if the MonetDB client library is available])],
# 		[]
# )

AC_PATH_PROG([MYSQL_CONFIG], [mysql_config], [no])
AS_IF([test "x$MYSQL_CONFIG" = "xno"], [
  AC_MSG_ERROR([mysql_config not found. Please install libmysqlclient-dev or mysql-client dev package.])
])

MYSQL_CFLAGS=`$MYSQL_CONFIG --cflags`
MYSQL_LIBS=`$MYSQL_CONFIG --libs`

AC_SUBST([MYSQL_CFLAGS])
AC_SUBST([MYSQL_LIBS])


# AC_ARG_ENABLE([asan],
#   [AC_HELP_STRING([--enable-asan], [Enable AddressSanitizer])],
#   [enable_asan=$enableval], [enable_asan=no])

# ASAN_FLAGS="-fsanitize=address"
# if test "$enable_asan" = "yes"; then
#   CXXFLAGS="$CXXFLAGS $ASAN_FLAGS"
#   LDFLAGS="$LDFLAGS $ASAN_FLAGS"
# fi

# AM_CONDITIONAL([DUT_MONETDB], [test x$pkg_failed = xno])

AX_BOOST_BASE()
AX_BOOST_REGEX

AC_SUBST(LIBPQXX_CFLAGS)
AC_SUBST(LIBPQXX_LIBS)

AC_SUBST(CONFIG_GIT_REVISION,
	[m4_esyscmd_s([git describe --dirty --tags --always 2>/dev/null || echo unknown])])

# AC_CHECK_LIB(sqlite3, sqlite3_open_v2)
# AM_CONDITIONAL([DUT_SQLITE], [test x$ac_cv_lib_sqlite3_sqlite3_open_v2 = xyes])

# AC_CHECK_LIB(mysqlclient, mysql_init)
# AM_CONDITIONAL([DUT_TIDB], [test x$ac_cv_lib_mysqlclient_mysql_init = xyes])

# AC_CHECK_LIB(mysqlclient, mysql_real_query_start)
# AM_CONDITIONAL([DUT_MARIADB], [test x$ac_cv_lib_mysqlclient_mysql_real_query_start = xyes])

# AC_CHECK_LIB(mysqlclient, mysql_real_query_nonblocking)
# AM_CONDITIONAL([DUT_MYSQL], [test x$ac_cv_lib_mysqlclient_mysql_real_query_nonblocking = xyes])

AC_SUBST(REVISION)

AC_CONFIG_HEADERS(config.h)
AC_CONFIG_FILES([
 Makefile
 Doxyfile
])
AC_OUTPUT

# ... existing code ...
# Add debug support
#AC_ARG_ENABLE([debug],
#    [AS_HELP_STRING([--enable-debug],
#        [Enable debug build (default is no)])],
#    [enable_debug=$enableval],
#    [enable_debug=no])

#AS_IF([test "x$enable_debug" = "xyes"], [
#    CXXFLAGS="$CXXFLAGS -g -O0"
#    AC_DEFINE([DEBUG], [1], [Enable debugging])
#], [
#    CXXFLAGS="$CXXFLAGS -O2"
#])
# ... existing code ...
