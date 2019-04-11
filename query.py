from moz_sql_parser import parse
import sys

from sql_params import parse_where, process_where, process_from, process_select, process_aggregate


aggr_list = {'max': 0, 'min': 0, 'sum': 0, 'avg': 0}
oper_list = {'and': 0, 'or': 0}
eq_list = {'gte': 0, 'lte': 0, 'eq': 0, 'gt': 0, 'lt': 0}


def main(query):
    schema = {}
    schema = read_meta_data(schema)
    process_query(query, schema)


def read_meta_data(schema):
    f = open('metadata.txt', 'r')
    start = False
    current_table = None

    for line in f:
        if line.strip() == "<begin_table>":
            start = True
            continue

        if start:
            current_table = line.strip()
            schema[current_table] = []
            start = False
            continue

        if not line.strip() == "<end_table>":
            schema[current_table].append(line.strip())

    return schema


def process_helper(query, schema, wildcard=False, d_flag=False):
    header_index, table_index, header = process_select(query, schema, wildcard)
    tuples = process_from(header_index, table_index)
    results = process_where(query, tuples, header_index, d_flag)
    data = print_data(results)
    return header, data


def print_data(results):
    data = ""
    for tup in results:
        for i in tup:
            data = data + str(i) + ','
        data = data[:-1] + "\n"
    return data[:-1]


def process(query, schema):
    if type(query['select']) is not list:
        query['select'] = [query['select']]

    if 'from' not in query:
        print("Syntax error. Did not specify from statement")
        exit()

    if type(query['from']) is not list:
        query['from'] = [query['from']]
    if 'where' in query:
        parse_where(query, schema)

    header = ""
    data = ""

    # select * from table_name
    if query['select'][0] == '*':
        query['select'] = []
        for table in query['from']:
            if table not in schema:
                print("Error. " + table + " does not exist")
                exit()
            for col in schema[table]:
                query['select'].append({'value': table + "." + col})
        header, data = process_helper(query, schema, wildcard=True)

    # select col1, col2 from table_name
    elif type(query['select'][0]['value']) is not dict:
        header, data = process_helper(query, schema)

    elif type(query['select'][0]['value']) is dict:
        # select distinct col1,col2 from table_name
        if 'distinct' in query['select'][0]['value']:
            if query['select'][0]['value']['distinct'] == '*':
                dis_cols = []
                for table in query['from']:
                    if table not in schema:
                        print("Error. " + table + " does not exist")
                        exit()
                    for col in schema[table]:
                        dis_cols.append({'value': table + "." + col})

                query['select'] = dis_cols
                header, data = process_helper(query, schema, wildcard=True, d_flag=True)
            else:
                dis_cols = [{'value': query['select'][0]['value']['distinct']}]
                if len(query['select']) > 1:
                    for i in range(1, len(query['select'])):
                        dis_cols.append({'value': query['select'][i]['value']})

                query['select'] = dis_cols
                print(query['select'])
                header, data = process_helper(query, schema, d_flag=True)

        # select min,max,avg,sum(col) from table_name
        else:
            header, data = process_aggregate(query, schema)

    return header, data


def process_query(query, schema):
    if query[-1] != ";":
        print("Syntax error. No ';' at the end.")
        exit()

    try:
        q_dict = parse(query[:-1])
        header, data = process(q_dict, schema)
        print(header)
        print(data)

    except Exception as e:
        print(e)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("No query given")
        exit()
    main(sys.argv[1])
