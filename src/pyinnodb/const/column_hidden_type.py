from enum import Enum


class ColumnHiddenType(Enum):
    HT_VISIBLE = 1  # the column is visible a normal column
    HT_HIDDEN_SE = 2  # not visible to the server
    HT_HIDDEN_SQL = (
        3  # visible to the server but hidden to the user, use for functional indexes
    )
    HT_HIDDEN_USER = 4  # visible to the server, but hidden to the user unless it is explicitly references in the stmt
