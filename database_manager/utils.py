# utf-8
# Python 3.7
# 2021-03-01


def columns2query(columns):
    """
    Transform list of columns to SQL query.

    Parameters:
        columns (list[str]) - list of column's names.

    Returns:
        query (str)

    Example:
        >> columns2query(['inn', 'org_name'])
        '"INN", "ORG_NAME"'
    """

    query = list(map(lambda x: '"' + x.upper() + '"', columns))
    query = ", ".join(query)

    return query
