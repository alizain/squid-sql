#! /usr/bin/env python3

import sys
import json
from collections import namedtuple

major, minor = sys.version_info[:2]
if not (major == 3 and minor >= 5):
    curr_version = '.'.join(map(str, sys.version_info[:3]))
    sys.stderr.write(
        f'Please use Python 3.5+ for this program.\nYou\'re trying to run it with Python {curr_version}.\n'
    )
    sys.exit(1)


class SquidError(Exception):
    component = ''

    def __init__(self, message, *args, **kwargs):
        super().__init__(message, *args, **kwargs)
        self.message = message


class SquidCLIError(SquidError):
    component = 'CLI'


SUPPORTED_COLUMN_TYPES = ['int', 'str']
SUPPORTED_OPS_BY_COLUMN_TYPE = {
    'int': ['=', '!=', '>', '>=', '<', '<='],
    'str': ['=', '!='],
}


def equal_to(left, right):
    return left == right


def not_equal_to(left, right):
    return left != right


def greater_than(left, right):
    return left > right


def greater_than_or_equal_to(left, right):
    return greater_than(left, right) or equal_to(left, right)


def less_than(left, right):
    return left < right


def less_than_or_equal_to(left, right):
    return less_than(left, right) or equal_to(left, right)


OPERATOR_FUNCS = {
    '=': equal_to,
    '!=': not_equal_to,
    '>': greater_than,
    '>=': greater_than_or_equal_to,
    '<': less_than,
    '<=': less_than_or_equal_to
}

ColumnConfig = namedtuple('ColumnConfig', ['table', 'name', 'type', 'index'])


class TableLoader:
    def __init__(self, table_class, filename_extra='.table.json'):
        self.table_class = table_class
        self.filename_extra = filename_extra
        self.table_cache = {}

    def name_to_filename(self, name):
        return f'{name}{self.filename_extra}'

    def load_table_from_file(self, table_name):
        filename = self.name_to_filename(table_name)
        try:
            with open(filename, 'r') as raw_data:
                raw_rows = json.load(raw_data)
                num_rows = len(raw_rows) - 1
                print(f'- Loaded "{filename}", {num_rows} rows.')
                table = self.table_class.from_file(table_name, raw_rows)
                self.table_cache[table_name] = table
                return table
        except FileNotFoundError:
            raise SquidError(f'table:{table_name} does not exist')

    def load_or_get_table(self, name):
        try:
            return self.table_cache[name]
        except KeyError:
            return self.load_table_from_file(name)


class Table:
    def __init__(self, columns, rows):
        self.columns = tuple(columns)
        self.rows = tuple(rows)

    def col_config_by_column_ref(self, column_ref):
        found_columns = []
        for col_config in self.columns:
            conditions = []
            if column_ref['table']:
                conditions.append(col_config.table == column_ref['table'])
            conditions.append(col_config.name == column_ref['name'])
            if all(conditions):
                found_columns.append(col_config)
        if len(found_columns) < 1:
            raise SquidError(
                f'column_ref:{column_ref} not found, maybe you haven\'t seleced the required table'
            )
        if len(found_columns) > 1:
            found_table_names = '|'.join(
                tuple(col_config.table for col_config in found_columns))
            raise SquidError(
                f'column_ref:{column_ref} is ambigious, found multiple matching columns in tables:{found_table_names}'
            )
        return found_columns[0]

    def apply_where_query(self, where_query):
        if isinstance(where_query, ColumnToLiteralWhere):
            return self.apply_column_to_literal_where_query(where_query)
        elif isinstance(where_query, ColumnToColumnWhere):
            return self.apply_column_to_column_where_query(where_query)
        else:
            raise SquidError(
                f'unknown where query type:{where_query.__class__}')

    def apply_column_to_column_where_query(self, where_query):
        left_col = self.col_config_by_column_ref(where_query.left['column'])
        right_col = self.col_config_by_column_ref(where_query.right['column'])
        if left_col.type != right_col.type:
            raise SquidError(
                f'cannot compare column:{left_col.name} of type:{left_col.type} with column:{right_col.name} of type:{right_col.type}'
            )
        if where_query.op not in SUPPORTED_OPS_BY_COLUMN_TYPE[left_col.type]:
            raise SquidError(
                f'non-sensical operator:{where_query.op} for type:{left_col.type}'
            )
        operator_func = OPERATOR_FUNCS[where_query.op]
        new_rows = []
        for row in self.rows:
            if operator_func(row[left_col.index], row[right_col.index]):
                new_rows.append(row)
        return self.__class__(self.columns, new_rows)

    def apply_column_to_literal_where_query(self, where_query):
        column_ref = where_query.column_term['column']
        col_config = self.col_config_by_column_ref(column_ref)
        literal_value_key = f'lit_{col_config.type}'
        if literal_value_key not in where_query.literal_term:
            raise SquidError(
                f'cannot compare column:{col_config.name} of type:{col_config.type} with term:{where_query.literal_term}'
            )
        if where_query.op not in SUPPORTED_OPS_BY_COLUMN_TYPE[col_config.type]:
            raise SquidError(
                f'non-sensical operator:{where_query.op} for type:{col_config.type}'
            )
        operator_func = OPERATOR_FUNCS[where_query.op]
        literal_value = where_query.literal_term[literal_value_key]
        new_rows = []
        if where_query.column_side == 'left':
            for row in self.rows:
                if operator_func(row[col_config.index], literal_value):
                    new_rows.append(row)
        elif where_query.column_side == 'right':
            for row in self.rows:
                if operator_func(literal_value, row[col_config.index]):
                    new_rows.append(row)
        else:
            raise Exception(
                'this should never happen because we\'ve already '
                'validated the `where` query when we created the '
                'ColumnToLiteralWhere class. If you\'ve arrived at '
                'this error, you haven\'t passed in a real '
                'ColumnToLiteralWhere, or you\'ve done some other '
                'really bad AbuseOfDynamicTyping™ thing!')
        return self.__class__(self.columns, new_rows)

    def apply_as_exp(self, as_name):
        new_columns = tuple(
            col_config._replace(table=as_name) for col_config in self.columns)
        return self.__class__(new_columns, self.rows)

    def apply_select_queries(self, raw_select_queries):
        found_columns = []
        found_names_by_col = {}
        for select_query in raw_select_queries:
            column_ref = select_query['source']['column']
            col_config = self.col_config_by_column_ref(column_ref)
            col_name = col_config.name
            if select_query['as']:
                col_name = select_query['as']
            if col_name in found_names_by_col.values():
                raise SquidError(
                    f'column as:{col_name} already `select`ed in this query, please use a unique name'
                )
            found_names_by_col[col_config] = col_name
            found_columns.append(col_config)
        found_indexes = tuple(col_config.index for col_config in found_columns)
        new_rows = self.filter_rows_by_column_indexes(found_indexes)
        new_columns = tuple(
            col_config._replace(index=i, name=found_names_by_col[col_config])
            for i, col_config in enumerate(found_columns))
        return self.__class__(new_columns, new_rows)

    def filter_rows_by_column_indexes(self, indexes_to_include):
        new_rows = []
        for existing_row in self.rows:
            row = [existing_row[i] for i in indexes_to_include]
            new_rows.append(row)
        return new_rows

    def printable_rows_with_column_headers(self):
        columns = tuple(col.name for col in self.columns)
        return (columns, self.rows)

    @classmethod
    def merge_tables(cls, one, two):
        to_add = len(one.columns)
        two_columns_fixed = tuple(
            col._replace(index=col.index + to_add) for col in two.columns)
        new_columns = one.columns + two_columns_fixed
        new_rows = []
        for one_row in one.rows:
            for two_row in two.rows:
                new_rows.append(one_row + two_row)
        return cls(new_columns, new_rows)

    @classmethod
    def from_file(cls, table_name, raw_rows):
        raw_columns = raw_rows.pop(0)
        columns = cls.validate_and_construct_col_configs(
            table_name, raw_columns)
        rows = tuple(raw_rows)
        return cls(columns, rows)

    @staticmethod
    def validate_and_construct_col_configs(table_name, raw_columns):
        columns = []
        for index, [col_name, col_type] in enumerate(raw_columns):
            col_config = ColumnConfig(
                name=col_name, type=col_type, table=table_name, index=index)
            columns.append(col_config)
        return tuple(columns)


class ColumnToLiteralWhere(
        namedtuple('ColumnToLiteralWhere',
                   ['op', 'left', 'right', 'column_side', 'literal_side'])):
    def __new__(cls, op, left, right):
        if 'column' in left and 'column' in right:
            raise SquidError(
                'this class may be what you want, but it\'s certainly '
                'not what you need, becasue what you gave me is not a '
                'column-to-literal where query - it\'s actually a '
                'column-to-column where query.')
        elif 'column' in left and 'column' not in 'right':
            return super().__new__(
                cls, op, left, right, column_side='left', literal_side='right')
        elif 'column' not in left and 'column' in 'right':
            return super().__new__(
                cls, op, left, right, column_side='right', literal_side='left')
        else:
            raise SquidError('malformed where expression, you doing okay?')

    @property
    def column_term(self):
        return getattr(self, self.column_side)

    @property
    def literal_term(self):
        return getattr(self, self.literal_side)


class ColumnToColumnWhere(
        namedtuple('ColumnToColumnWhere', ['op', 'left', 'right'])):

    pass


def run_query(table_loader, raw_query):
    raw_from_queries = raw_query['from']
    raw_select_queries = raw_query['select']
    raw_where_queries = raw_query.get('where', [])

    tables = []

    # run `from` queries
    used_table_names = []
    for from_query in raw_from_queries:
        source_table_name = from_query['source']['file']
        new_table = table_loader.load_or_get_table(source_table_name)
        new_table_name = from_query['as'] or source_table_name
        if new_table_name in used_table_names:
            raise SquidError(
                f'table:{new_table_name} has already been declared in this query, try using `as` to give it a distinct name'
            )
        if new_table_name != source_table_name:
            new_table = new_table.apply_as_exp(from_query['as'])
        used_table_names.append(new_table_name)
        tables.append(new_table)

    # do the shortcut merge upfront
    query_table = tables.pop(0)
    while tables:
        table_to_merge = tables.pop(0)
        query_table = Table.merge_tables(query_table, table_to_merge)

    # run `where` queries
    if raw_where_queries:
        all_where_queries = build_where_queries(raw_where_queries)
        for where_query in all_where_queries:
            query_table = query_table.apply_where_query(where_query)

    query_table = query_table.apply_select_queries(raw_select_queries)
    pretty_print_table(query_table)


def build_where_queries(raw_where_queries):
    built_queries = []
    for where_query in raw_where_queries:
        if 'column' in where_query['left'] and 'column' in where_query['right']:
            built_queries.append(ColumnToColumnWhere(**where_query))
        else:
            built_queries.append(ColumnToLiteralWhere(**where_query))
    return built_queries


def pretty_print_table(query_table):
    columns, all_rows = query_table.printable_rows_with_column_headers()
    col_divider = ' | '
    col_max_widths = find_col_max_widths((columns, ) + all_rows)
    col_formatters = create_col_formatters(col_max_widths)
    divider_widths = (len(col_max_widths) - 1) * len(col_divider)
    total_max_width = sum(col_max_widths) + divider_widths
    str_row_divider = '-' * total_max_width
    str_headers = stringify_row(col_divider, col_formatters, columns)
    str_rows = tuple(
        stringify_row(col_divider, col_formatters, row) for row in all_rows)
    print('\n'.join((str_headers, str_row_divider) + str_rows))


def stringify_row(divider, formatters, row):
    row_stringified = (formatter.format(value)
                       for formatter, value in zip(formatters, row))
    return divider.join(row_stringified)


def create_col_formatters(col_max_widths):
    all_but_last = tuple(f'{{:<{width}}}' for width in col_max_widths[:-1])
    last_width = col_max_widths[-1]
    return all_but_last + (f'{{:>{last_width}}}', )


def find_col_max_widths(all_rows):
    num_columns = len(all_rows[0])
    col_max_widths = [0] * num_columns
    for row in all_rows:
        for index in range(num_columns):
            curr_len = len(str(row[index]))
            if curr_len > col_max_widths[index]:
                col_max_widths[index] = curr_len
    return col_max_widths


def parse_query_file_and_run_query(filename):
    try:
        with open(filename) as query_file:
            table_loader = TableLoader(table_class=Table)
            curr_query = json.load(query_file)
            run_query(table_loader, curr_query)
    except FileNotFoundError:
        raise SquidError(
            f'query file:{filename} not found, please make sure it exists')


if __name__ == '__main__':
    try:
        query_file_name = ''
        try:
            query_file_name = sys.argv[1]
        except IndexError:
            raise SquidCLIError('no query file provided')
        parse_query_file_and_run_query(query_file_name)
    except SquidError as err:
        if err.component:
            print(f'ERROR({err.component}): {err.message}')
        else:
            print(f'ERROR: {err.message}')
