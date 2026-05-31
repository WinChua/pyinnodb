from enum import Enum


class ColumnHiddenType(Enum):
    """
    use when drop column
    """

    HT_VISIBLE = 0  # the column is visible as a normal column (default)
    HT_HIDDEN_SE = 1  # not visible to the server
    HT_HIDDEN_SQL = (
        2  # visible to the server but hidden to the user, use for functional indexes
    )
    HT_HIDDEN_USER = 3  # visible to the server, but hidden to the user unless it is explicitly referenced in the stmt
