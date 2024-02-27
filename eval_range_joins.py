from eval_single_table import convert_tables, read_tables_new
from grid_index.multidimensional_grid import *
from grid_index import range_filter
import pickle
import os

if __name__ == '__main__':
    """which tables need to be read"""
    t = 'customer'

    # some setting for grid index
    dimensions_to_ignore = []
    ranges_per_dimension = []
    column_indxs_for_ar_model_training = []
    column_names = ''
    header_flag = False
    if t == 'customer':
        # "customer": ["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT"]
        dimensions_to_ignore = [1, 2, 4, 6]
        # this is both the dimensions to ignore and the dimensions to delete which are set in the read_tables method
        dims_to_delete_for_querying = [1, 2, 4, 6]
        """for the ar model, we are only interested in training over the columns that can have equalities and not others"""
        # this tells us about how many ranges we want to create for a dimension
        ranges_per_dimension = [5, 0, 0, 10, 0, 5, 0]

        column_indxs_for_ar_model_training = [1, 2, 4, 6]
        column_names = 'grid_cell,c_name,c_address,c_phone,c_mktsegment'
        separator = '|'

    if data_location_variables.read_parsed_dataset and os.path.exists('./table_objects/{}.pickle'.format(t)):
        with open('./table_objects/{}.pickle'.format(t), 'rb') as existing_obj:
            table_of_interest = pickle.load(existing_obj)
    else:
        tables = read_tables_new([t], dataset_name=data_location_variables.dataset_name, header_flag=header_flag,
                                 separator=separator)

        # convert the table
        convert_tables(tables)

        table_of_interest = tables[t]
        # store the dataset
        if data_location_variables.store_parsed_dataset:
            with open('./table_objects/{}.pickle'.format(t), 'wb') as store_obj:
                pickle.dump(table_of_interest, store_obj, protocol=pickle.HIGHEST_PROTOCOL)

    relevant_ranges_only = [range_tmp
                            for range_indx, range_tmp in enumerate(ranges_per_dimension)
                            if range_indx not in column_indxs_for_ar_model_training]

    if not data_location_variables.load_existing_grid:
        example_data = list(zip(*table_of_interest.new_rows))

        grid_index = MultidimensionalGrid(example_data, relevant_ranges_only, cdf_based=True,
                                          dimensions_to_ignore=dimensions_to_ignore,
                                          column_indxs_for_ar_model_training=column_indxs_for_ar_model_training,
                                          column_names=column_names, table_name=table_of_interest.table_name,
                                          table_size=len(table_of_interest.new_rows))

        if data_location_variables.store_grid:
            # grid_index.estimator = None # TODO: uncomment to store just the grid without the ar model to measure grid memory
            with open('index_' + str(table_of_interest.table_name) + data_location_variables.grid_ar_name,
                      'wb') as handle:
                pickle.dump(grid_index, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        print("Loading an existing index: ")
        with open('index_' + str(t) + data_location_variables.grid_ar_name, 'rb') as handle:
            grid_index = pickle.load(handle)


    # queries_file_name = "queries_generator/range_queries/min_max_files/all_inequality_{}.pickle"  # all inequality
    # queries_file_name = "queries_generator/range_queries/min_max_files/all_range_{}.pickle"  # all range
    queries_file_name = "queries_generator/range_queries/min_max_files/all_queries_{}.pickle"  # all queries
    all_queries_min = list()
    with open(queries_file_name.format('min'), 'rb') as read_file:
        all_queries_min = pickle.load(read_file)

    all_queries_max = list()
    with open(queries_file_name.format('max'), 'rb') as read_file:
        all_queries_max = pickle.load(read_file)

    change_of_join_operator = list()
    query_results = list()
    # with open('queries_generator/range_queries/all_queries_inequality_original.sql', 'r') as queries_path: # all inequality
    # with open('queries_generator/range_queries/all_queries_range_original.sql', 'r') as queries_path: # all range
    with open('queries_generator/range_queries/all_queries_original.sql', 'r') as queries_path: # all queries
        for line_id, line in enumerate(queries_path):
            if line.strip() == '-end-':
                change_of_join_operator.append(line_id - len(change_of_join_operator))
                continue
            query, query_result = line.split("|")
            query_result = int(query_result)

            query_results.append(query_result)

    range_join_results = list()
    """this will be all the files of interest"""
    list_of_all_files = list()
    """customer"""
    # all queries
    list_of_all_files.append('queries_generator/range_queries/all_queries_range.sql')
    # inequality queries
    # list_of_all_files.append('queries_generator/range_queries/all_queries_inequality_range.sql')
    # range queries
    # list_of_all_files.append('queries_generator/range_queries/all_queries_range_range.sql')

    """these will be all the range objects of interest"""
    range_objects_of_interest = dict()
    id_of_object = 0

    # with open('queries_generator/range_queries/range_join_conditions/range_inequality_queries_all.txt', 'r') as range_objects: # all inequality queries
    # with open('queries_generator/range_queries/range_join_conditions/range_range_queries_all.txt', 'r') as range_objects: # all range queries
    with open('queries_generator/range_queries/range_join_conditions/all_queries_range.txt', 'r') as range_objects: # all join queries
        tmp_lines = list()
        for line in range_objects:
            if line.strip() != '-end-':
                tmp_lines.append(line.strip())
            else:
                range_objects_of_interest[id_of_object] = tmp_lines
                tmp_lines = list()
                id_of_object += 1

    for range_file in list_of_all_files:
        with open(range_file, 'r') as queries_path:
            for line_id, line in enumerate(queries_path):
                if line.strip() != '-end-':
                    query, query_result = line.split("|")
                    query_result = int(query_result)

                    range_join_results.append(query_result)

    column_names_split = [c_name for c_name in column_names.split(',')]

    grid_index_query_min = list()
    grid_index_query_max = list()
    ar_model_queries = list()

    ar_model_columns_indexes = list()
    for ar_model_cols in column_names_split[1:]:
        if ar_model_cols.upper() in table_of_interest.header:
            ar_model_columns_indexes.append(table_of_interest.header.index(ar_model_cols.upper()))
        elif ar_model_cols.lower() in table_of_interest.header:
            ar_model_columns_indexes.append(table_of_interest.header.index(ar_model_cols.lower()))

    column_indexes_of_cols_not_in_ar_model = list()
    for col_name in table_of_interest.header:
        if col_name.lower() not in column_names_split:
            column_indexes_of_cols_not_in_ar_model.append(table_of_interest.header.index(col_name))

    print(f'there are {len(all_queries_min)} queries')

    dimensions_to_ignore.sort(reverse=True)
    dims_to_delete_for_querying.sort(reverse=True)

    for query_indx, query_min in enumerate(all_queries_min):
        query_max = all_queries_max[query_indx]
        '''part for the ar_model'''
        ar_model_query = [None] * (len(column_names_split) - 1)
        ar_model_query = [-1] + ar_model_query

        exact_query_min = query_min.copy()
        exact_query_max = query_max.copy()

        '''part for the grid index'''
        grid_index_min = query_min.copy()
        grid_index_max = query_max.copy()


        for col_indx, col_name in enumerate(column_names_split[1:]):
            """
                take the query value for the columns that are mapped and 
                map it to the value used internally for the models
            """
            if exact_query_min[ar_model_columns_indexes[col_indx]] is not None:
                # for the min part
                if col_name.upper() in table_of_interest.column_mapper.keys():
                    min_val_map = table_of_interest.column_mapper[col_name.upper()][
                        exact_query_min[ar_model_columns_indexes[col_indx]]]
                elif col_name.lower() in table_of_interest.column_mapper.keys():
                    min_val_map = table_of_interest.column_mapper[col_name.lower()][
                        exact_query_min[ar_model_columns_indexes[col_indx]]]

                exact_query_min[ar_model_columns_indexes[col_indx]] = min_val_map

                # for the max part
                exact_query_max[ar_model_columns_indexes[col_indx]] = min_val_map

                ar_model_query[col_indx + 1] = min_val_map

        # not relevant cols
        for dim_to_delete in dims_to_delete_for_querying:
            del grid_index_min[dim_to_delete]
            del grid_index_max[dim_to_delete]

        # col mappings
        for dim, _ in enumerate(grid_index_min):
            if grid_index_min[dim] is None:
                grid_index_min[dim] = grid_index.min_per_dimension[dim]
                grid_index_max[dim] = grid_index.max_per_dimension[dim]
            else:
                col_name = table_of_interest.header[column_indexes_of_cols_not_in_ar_model[dim]]

                if col_name.upper() in table_of_interest.column_mapper.keys():
                    min_val_map = table_of_interest.column_mapper[col_name.upper()][
                        grid_index_min[dim]]

                    max_val_map = table_of_interest.column_mapper[col_name.upper()][
                        grid_index_max[dim]]

                    grid_index_min[dim] = min_val_map
                    grid_index_max[dim] = max_val_map
                elif col_name.lower() in table_of_interest.column_mapper.keys():
                    min_val_map = table_of_interest.column_mapper[col_name.lower()][
                        grid_index_min[dim]]

                    max_val_map = table_of_interest.column_mapper[col_name.lower()][
                        grid_index_max[dim]]

                    grid_index_min[dim] = min_val_map
                    grid_index_max[dim] = max_val_map

        grid_index_query_min.append(grid_index_min)
        grid_index_query_max.append(grid_index_max)
        ar_model_queries.append(ar_model_query)


    print('ESTIMATING------------------------------------:')
    q_error = list()
    q_error_original = list()
    time_total = list()
    time_total_grid = list()
    time_total_join = list()
    time_total_exact = list()
    other_query_index = 0
    relative_error_range = list()

    id_of_range_object_of_interest = 0
    total_num_queries_checked = 0

    for query_indx, query_min in enumerate(grid_index_query_min):
        if query_indx != 0 and query_indx in change_of_join_operator:
            """take the right join operator"""
            id_of_range_object_of_interest += 1

        # query without join predicates result
        true_result = query_results[query_indx]

        # TODO: uncomment and modify range to evaluate queries based on selectivity of query predicates
        # if not true_result < 1000:
        #     other_query_index += 1
        #     print(f'Query predicate result is {true_result}, so continuing')
        #     continue

        total_num_queries_checked += 1

        print(f'working with query {query_indx + 1}')
        query_max = grid_index_query_max[query_indx]
        ar_query = ar_model_queries[query_indx]

        """estimating grid cells for first table"""
        time_s_grid_1 = time.time()
        qualifying_cells_1, estimated_cardinality_1 = grid_index.range_join_qualifying_cells(query_min_vals=query_min,
                                                                                         query_max_vals=query_max,
                                                                                         values_ar_model=ar_query,
                                                                                         columns_for_ar_model=column_names_split)
        time_e_grid_1 = (time.time() - time_s_grid_1) * 1000
        qualifying_cells_1 = np.array(qualifying_cells_1)
        print(f"There are {len(qualifying_cells_1)} qualifying cells! found for {time_e_grid_1}")

        # """estimating grid cells for second table"""
        time_s_grid_2 = time.time()
        qualifying_cells_2, estimated_cardinality_2 = grid_index.range_join_qualifying_cells(query_min_vals=query_min,
                                                                                             query_max_vals=query_max,
                                                                                             values_ar_model=ar_query,
                                                                                             columns_for_ar_model=column_names_split)
        time_e_grid_2 = (time.time() - time_s_grid_2) * 1000
        qualifying_cells_2 = np.array(qualifying_cells_2)
        print(f"There are {len(qualifying_cells_2)} qualifying cells! found for {time_e_grid_2}")

        """creating range join objects in the required processing form of Grid-AR"""
        rps = []
        for range_pred_data in range_objects_of_interest[id_of_range_object_of_interest]:
            split_elems = range_pred_data.split('|')
            range_join_sign = split_elems[0].strip()
            range_join_col_ids = list()
            for range_join_col_id in split_elems[1].strip().split(','):
                range_join_col_ids.append(int(range_join_col_id))
            range_join_condition_first_table = split_elems[2].strip()
            range_join_condition_second_table = split_elems[3].strip()

            tmp_rp = range_filter.RangeJoinPredicate(range_join_sign, column_ids=range_join_col_ids,
                                                     expressions=[range_join_condition_first_table,
                                                                  range_join_condition_second_table])
            rps.append(tmp_rp)

        if len(qualifying_cells_1) < 100:
            buckets2_card = np.array([])
            for i, b in enumerate(qualifying_cells_2):
                b.id = i
                buckets2_card = np.append(buckets2_card, b.num_points)
            time_s = time.time()
            res = 0

            for bucket1 in qualifying_cells_1:
                res += range_filter.process_range(qualifying_cells_2, rps, buckets2_card, bucket1)

            time_e = (time.time() - time_s) * 1000
        else:
            max_workers = 3 # Number of worker threads
            chunk_size = int(len(qualifying_cells_1)/max_workers) # Size of each worker chunk
            time_s = time.time()
            res = range_filter.parallel_execution(chunk_size, max_workers, qualifying_cells_1, qualifying_cells_2, rps)
            time_e = (time.time() - time_s) * 1000

        res = res if res > 0 else 1

        # take both the time from the grid and the time for the cells
        time_total.append(time_e + (time_e_grid_1 + time_e_grid_2))
        time_total_grid.append(time_e_grid_1 + time_e_grid_2)
        time_total_join.append(time_e)

        # this is just using the existing result
        true_range_result = range_join_results[other_query_index]

        res = math.ceil(res)
        print(f'Estimated query cardinality {estimated_cardinality_1} out of {true_result}')
        print(f'Estimated query cardinality {estimated_cardinality_2} out of {true_result}')
        print(f'Estimated range cardinality {res} out of {true_range_result} for {time_e + (time_e_grid_1 + time_e_grid_2)} MS')

        q_error.append(max(res / true_range_result, true_range_result / res))
        relative_error_range.append(abs((res - true_range_result) / res))
        q_error_original.append(max(estimated_cardinality_1 / true_result, true_result / estimated_cardinality_1))
        other_query_index += 1

    """THIS IS FOR THE OUTER WHILE that iterates over the different files"""
    print(f"-------------------------------------------------")

    """PRINTING THE RESULTS"""
    print(f'Average q-error {np.average(q_error)}')
    print(f'Median q-error {np.quantile(q_error, .5)}')
    print(f'90 percentile q-error {np.quantile(q_error, .9)}')
    print(f'99 percentile q-error {np.quantile(q_error, .99)}')
    print(f'Max q-error {np.quantile(q_error, 1.)}')
    print(f'Average relative error {np.average(relative_error_range)}')
    print(f'Median relative error {np.quantile(relative_error_range, .5)}')
    print(f'90 percentile relative error {np.quantile(relative_error_range, .9)}')
    print(f'99 percentile relative error {np.quantile(relative_error_range, .99)}')
    print(f'Max relative error {np.quantile(relative_error_range, 1.)}')
    print(f'Our approach avg execution time: {np.average(time_total)} MS; grid {np.average(time_total_grid)} MS; join {np.average(time_total_join)} MS')
    print(f'Our approach median execution time: {np.quantile(time_total, .5)} MS; grid {np.quantile(time_total_grid, .5)} MS; join {np.quantile(time_total_join, .5)} MS')
    print(f'Our approach minimal execution time: {np.quantile(time_total, .0)} MS; grid {np.quantile(time_total_grid, .0)} MS; join {np.quantile(time_total_join, .0)} MS')
    print(f'Our approach maximal execution time: {np.quantile(time_total, 1.)} MS; grid {np.quantile(time_total_grid, 1.)} MS; join {np.quantile(time_total_join, 1.)} MS')

    print(f'Average q-error original {np.average(q_error_original)}')
    print(f'Median q-error original {np.quantile(q_error_original, .5)}')
    print(f'Total number of queries checked {total_num_queries_checked}')