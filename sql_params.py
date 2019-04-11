import csv
from itertools import product
import sys


aggr_list = {'max': 0, 'min': 0, 'sum': 0, 'avg': 0}
oper_list = {'and': 0, 'or': 0}
eq_list = {'gte': 0, 'lte': 0, 'eq': 0, 'gt': 0, 'lt': 0}


####################################################################################################
#                                        SELECT PROCESSING                                         #
####################################################################################################
def process_select(query, schema, wildcard=False):
    bl_table = ""
    bl_index = ""
    if 'where' in query:
        join_params = check_join(query)
        if join_params[0]:
            bl_table = join_params[1]
            bl_index = join_params[2]

    header_index = {}
    table_index = []
    for table in query['from']:
        header_index[table] = []
        table_index.append(table)
    header = "<"

    for v in query['select']:
        found = False
        if "." in v['value']:
            table_found = False
            for table_name in query['from']:

                if table_name not in schema:
                    print("Error. " + table_name + " does not exist")
                    exit()

                for i, col in enumerate(schema[table_name]):
                    tokens = v['value'].split('.')

                    if tokens[0] not in schema:
                        print("Error. " + tokens[0] + " does not exist")
                        exit()

                    if tokens[0] == table_name:
                        if col == tokens[1]:
                            if table_name == bl_table and i == bl_index and wildcard:
                                table_found = True
                                found = True
                                continue
                            header = header + table_name + '.' + col + ','
                            header_index[table_name].append(i)
                            found = True
                            table_found = True
                            break
            if not table_found:
                print("Error. " + v['value'].split('.')[0] + " does not exist")
                exit()

        else:
            table_count = 0
            for table_name in query['from']:

                if table_name not in schema:
                    print("Error. " + table_name + " does not exist")
                    exit()

                for i, col in enumerate(schema[table_name]):
                    if col == v['value']:
                        if table_count > 0:

                            if not wildcard:
                                print("Error. Ambiguity in " + col + ". Table not specified")
                                exit()

                        if table_name == bl_table and i == bl_index and wildcard:
                            continue
                        header = header + table_name + '.' + col + ','
                        header_index[table_name].append(i)
                        table_count = table_count + 1
                        found = True
                        break

        if not found:
            print("Error. '" + v['value'] + "' does not exist in any table")
            exit()

    header = header[:-1] + ">"
    return header_index, table_index, header

####################################################################################################
####################################################################################################


####################################################################################################
#                                        FROM PROCESSING                                           #
####################################################################################################
def product_dict(**kwargs):
    keys = kwargs.keys()
    vals = kwargs.values()
    for instance in product(*vals):
        yield dict(zip(keys, instance))


def process_from(header_index, table_index):
    full_data = {}
    for i in range(len(header_index)):
        fpi = open(table_index[i] + '.csv', 'r')
        data_reader_i = csv.reader(fpi)
        full_data[table_index[i]] = []
        for row_i in data_reader_i:
            full_data[table_index[i]].append(row_i)

    tuples = list(product_dict(**full_data))
    return tuples

####################################################################################################
####################################################################################################


####################################################################################################
#                                       WHERE PROCESSING                                           #
####################################################################################################
def parse_where_help(params, schema, query):
    if type(params) == str:
        if "." in params:
            tokens = params.split(".")

            if tokens[0] not in schema:
                print("Error. " + tokens[0] + " does not exist")
                exit()

            for j, col in enumerate(schema[tokens[0]]):
                if col == tokens[1]:
                    return tokens[0], j
            print("Error. '" + tokens[1] + "' is not in " + tokens[0])
            exit()
        else:
            table_count = 0
            table_name = ""
            temp = {}
            for table in query['from']:

                if table not in schema:
                    print("Error. " + table + " does not exist")
                    exit()

                temp[table] = []
                for j, col in enumerate(schema[table]):
                    if col == params:

                        if table_count > 0:
                            print("Error. Ambiguity in " + col + ". Table not specified")
                            exit()

                        temp[table].append(j)
                        table_count = table_count + 1
                        table_name = table
                        break
            if table_count == 1:
                return table_name, temp[table_name][0]
            print("Error. '" + params + "' is not in any mentioned table")
            exit()


def parse_where(query, schema):
    operator = ""
    for k in oper_list:
        if k in query['where']:
            operator = k
            break

    if not operator:
        eq = ""
        for k in query['where']:
            if k in query['where']:
                eq = k
                break

        for i, params in enumerate(query['where'][eq]):
            if type(params) == str:
                query['where'][eq][i] = parse_where_help(params, schema, query)

    else:
        for c, rel in enumerate(query['where'][operator]):
            for eq, p in rel.items():
                for i, params in enumerate(p):
                    if type(params) == str:
                        query['where'][operator][c][eq][i] = parse_where_help(params, schema, query)


def make_bool(lit, eq, row):
    lit0 = lit[0]
    if type(lit0) == tuple:
        lit0 = int(row[lit0[0]][lit0[1]])
    lit1 = lit[1]
    if type(lit1) == tuple:
        lit1 = int(row[lit1[0]][lit1[1]])
    if eq == 'eq':
        return lit0 == lit1
    if eq == 'gt':
        return lit0 > lit1
    if eq == 'lt':
        return lit0 < lit1
    if eq == 'gte':
        return lit0 >= lit1
    if eq == 'lte':
        return lit0 <= lit1


def check_where(query, row):
    operator = ""
    for k in oper_list:
        if k in query['where']:
            operator = k
            break

    if not operator:
        eq = ""
        for k in query['where']:
            if k in query['where']:
                eq = k
                break
        return make_bool(query['where'][eq], eq, row)

    else:
        bools = []
        for eq in query['where'][operator]:
            for k in eq:
                bools.append(make_bool(eq[k], k, row))
        if operator == 'and':
            return bools[0] and bools[1]
        elif operator == 'or':
            return bools[0] or bools[1]


def check_join(query):
    operator = ""
    for k in oper_list:
        if k in query['where']:
            operator = k
            break

    if not operator:
        eq = ""
        for k in query['where']:
            if k in query['where']:
                eq = k
                break
        lit = query['where'][eq]
        if type(lit[0]) == tuple and type(lit[1]) == tuple and eq == 'eq':
            if lit[0][1] == lit[1][1]:
                return True, lit[1][0], lit[1][1]

    else:
        for eq in query['where'][operator]:
            for k in eq:
                lit = eq[k]
                if type(lit[0]) == tuple and type(lit[1]) == tuple and k == 'eq':
                    if lit[0][1] == lit[1][1]:
                        return True, lit[1][0], lit[1][1]
    return False, "", ""


def process_where(query, tuples, header_index, d_flag):
    if d_flag:
        results = set()
    else:
        results = []
    for t in tuples:
        if 'where' in query:
            if not check_where(query, t):
                continue

        temp = ()
        for table in header_index:
            for i in header_index[table]:
                temp = temp + (t[table][i],)
        if d_flag:
            results.add(temp)
        else:
            results.append(temp)
    return results

####################################################################################################
####################################################################################################


####################################################################################################
#                                   AGGREGATE PROCESSING                                           #
####################################################################################################
def process_aggregate(query, schema):
    func = ""
    for k in query['select'][0]['value']:
        if k in query['select'][0]['value']:
            func = k
            break

    if func not in aggr_list:
        print("Error. '" + func + "' is not a valid function")
        exit()

    col = query['select'][0]['value'][func]

    header_index = {}
    table_index = []
    header = ""
    for table in query['from']:
        header_index[table] = []
        table_index.append(table)

    found = False
    if "." in col:
        tokens = col.split('.')
        table_name = tokens[0]

        if table_name not in schema:
            print("Error. " + table_name + " does not exist")
            exit()

        for i, cols in enumerate(schema[table_name]):
            if tokens[1] == cols:

                if table_name not in header_index:
                    print("Error. table name in from does not match.")
                    exit()

                header_index[table_name].append(i)
                found = True
                break
        header = '<' + func + '(' + col + ')>'
    else:
        table_count = 0
        table_name = ""
        for table in query['from']:
            if table not in schema:
                print("Error. " + table + " does not exist")
                exit()

            for i, cols in enumerate(schema[table]):
                if col == cols:
                    if table_count == 0:
                        header_index[table].append(i)
                        table_name = table
                        table_count = table_count + 1
                        found = True
                        break
                    else:
                        print("Error. Ambiguity in " + col + ". Table not specified")
                        exit()
            header = '<' + func + '(' + table + '.' + col + ')>'

    if not found:
        print("Error. '" + col + "' does not exist in any table")
        exit()

    data = 0
    tuples = process_from(header_index, table_index)

    if func == 'max':
        max_col = -sys.maxsize - 1
        for t in tuples:
            if 'where' in query:
                if not check_where(query, t):
                    continue
            temp = int(t[table_name][header_index[table_name][0]])
            if max_col < temp:
                max_col = temp
        data = max_col

    if func == 'min':
        min_col = sys.maxsize
        for t in tuples:
            if 'where' in query:
                if not check_where(query, t):
                    continue
            temp = int(t[table_name][header_index[table_name][0]])
            if min_col > temp:
                min_col = temp
        data = min_col

    if func == 'avg':
        avg_col = 0
        len_col = 0
        for t in tuples:
            if 'where' in query:
                if not check_where(query, t):
                    continue
            avg_col = avg_col + int(t[table_name][header_index[table_name][0]])
            len_col = len_col + 1
        data = avg_col / len_col

    if func == 'sum':
        sum_col = 0
        for t in tuples:
            if 'where' in query:
                if not check_where(query, t):
                    continue
            sum_col = sum_col + int(t[table_name][header_index[table_name][0]])
        data = sum_col

    return header, data

####################################################################################################
####################################################################################################
