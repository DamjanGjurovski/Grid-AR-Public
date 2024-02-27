import numpy as np
from scipy import integrate
import numexpr as ne
from multiprocessing import Pool
from functools import partial

class RangeJoinPredicate:
    def __init__(self, operator, column_ids, expressions=None, range_first_col=None, range_second_col=None,
                 num_cells_first_col=None, num_cells_second_col=None):
        """
        The class containing the Range Join Predicates between the tables
        :param operator: Possible operators are [<, >, >=, <=]
        :param column_ids: Ids of the columns involved in the predicate
        :param expressions: Expressions that will shift the boundaries of the buckets, example x1 * 3 + 2
        """
        self.operator = operator
        self.column_ids = column_ids
        self.expressions = expressions
        self.type = 0
        self.range_first_col = range_first_col
        self.range_second_col = range_second_col
        self.num_cells_first_col = num_cells_first_col
        self.num_cells_second_col = num_cells_second_col

        if range_first_col is not None:
            a = range_first_col
            self.updated_expressions_first_col = ne.evaluate(self.expressions[0])

            b = range_second_col
            self.updated_expressions_second_col = ne.evaluate(self.expressions[1])


    def compute_overlap(self):
        common_area = min(self.updated_expressions_first_col[1], self.updated_expressions_second_col[1]) - \
                      max(self.updated_expressions_first_col[0], self.updated_expressions_second_col[0])


        whole_area = max(self.updated_expressions_first_col[1], self.updated_expressions_second_col[1]) - \
                     min(self.updated_expressions_first_col[0], self.updated_expressions_second_col[0])


        return common_area / whole_area

    def __eq__(self, other):
        """
        Checking if the first is greater than the second. If the second has more
        :param other:
        :return:
        """
        if not isinstance(other, RangeJoinPredicate):
            raise TypeError('can only compare two RangeJoinPredicates')

        if self.compute_overlap() == other.compute_overlap():
            return True
        return False

    def __gt__(self, other):
        """
        Checking if the first is greater than the second. If the second has more
        :param other:
        :return:
        """
        if not isinstance(other, RangeJoinPredicate):
            raise TypeError('can only compare two RangeJoinPredicates')

        if self.compute_overlap() < other.compute_overlap():
            return False

        return True

    def __lt__(self, other):
        """
        Checking if the first is lower than the second.
        :param other:
        :return:
        """
        if not isinstance(other, RangeJoinPredicate):
            raise TypeError('can only compare two RangeJoinPredicates')

        if self.compute_overlap() < other.compute_overlap():
            return True

        return False

class GridCellResult:
    def __init__(self, id, min_boundaries, max_boundaries, est_cell_cardinality=None, per_dimension_distribution=None):
        self.id = id
        self.min_boundaries = np.array(min_boundaries)
        self.max_boundaries = np.array(max_boundaries)
        self.num_points = est_cell_cardinality
        self.per_dimension_distribution = per_dimension_distribution
        bounding_box_volume_data = (self.max_boundaries - self.min_boundaries)
        self.bounding_box_volume = np.product([i for i in bounding_box_volume_data if i != 0])

    def __str__(self):
        return str(self.id) + ", " + str(self.min_boundaries) + ", " + str(self.max_boundaries) + ": " + str(self.num_points)

    def compute_volume(self, query):
        range_start = np.max([query[0], self.min_boundaries], axis=0)
        range_end = np.min([query[1], self.max_boundaries], axis=0)
        volume_overlap = 1.0
        for i in range(len(range_start)):
            if range_end[i] < range_start[i]:
                return 0
            else:
                diff = (range_end[i] - range_start[i])
                if diff != 0:
                    volume_overlap *= diff
        return volume_overlap / self.bounding_box_volume

def process_range(buckets2, range_join_predicates, buckets2_card, bucket1):
    """
    For a cell from one table, check all cells from the other table.
    :param buckets2: cells from the second table
    :param range_join_predicates: join conditions of the query
    :param buckets2_card: estimated cardinality for the cells of the second table
    :param bucket1: cell from the first table
    :return:
    """
    total_card = 0

    overlap = np.ones(len(buckets2)) * bucket1.num_points

    for p_i, predicate in enumerate(range_join_predicates):
        a = bucket1.min_boundaries[predicate.column_ids[0]]
        min_x = ne.evaluate(predicate.expressions[0])
        a = bucket1.max_boundaries[predicate.column_ids[0]]
        max_x = ne.evaluate(predicate.expressions[0])

        # TODO UNCOMMENT
        # if p_i != 0 and zero_bucket_id != -1:
        #     buckets2 = buckets2[zero_bucket_id : len(buckets2)]
            # print(zero_bucket_id)
            # print("Going inside")
            # print(len(buckets2))
            # exit(1)
        # zero_bucket_id = -1 # TODO UNCOMMENT

        """sort buckets based on the operator"""
        if predicate.operator == ">":
            buckets2 = sorted(buckets2, key=lambda x: x.max_boundaries[predicate.column_ids[1]], reverse=True)

        else:
            buckets2 = sorted(buckets2, key=lambda x: x.min_boundaries[predicate.column_ids[1]], reverse=False)

        for j, rect_j in enumerate(buckets2):
            if overlap[rect_j.id] == 0:
                continue
            # apply the join condition
            b = rect_j.min_boundaries[predicate.column_ids[1]]
            min_y = ne.evaluate(predicate.expressions[1])
            b = rect_j.max_boundaries[predicate.column_ids[1]]
            max_y = ne.evaluate(predicate.expressions[1])

            if predicate.operator == "<":
                if max_x <= min_y:
                    break
                elif min_x >= max_y:
                    overlap[rect_j.id] = 0
                    # zero_bucket_id = j # TODO UNCOMMENT
                else:
                    # TODO: uncomment if the distribution of the columns in the cells is known
                    # overlap[rect_j.id] *= overlap_calculation(min_x, max_x, min_y, max_y,
                    #                                           x_cell_obj=bucket1.per_dimension_distribution[predicate.column_ids[0]],
                    #                                           y_cell_obj=rect_j.per_dimension_distribution[predicate.column_ids[1]])

                    # distribution of columns in cells is unknown
                    overlap[rect_j.id] *= overlap_calculation(min_x, max_x, min_y, max_y)
            else:
            # elif predicate.operator == ">":
                if min_x >= max_y:
                    break
                elif max_x <= min_y:
                    overlap[rect_j.id] = 0
                    # zero_bucket_id = j # TODO UNCOMMENT
                else:
                    # TODO: uncomment if the distribution of the columns in the cells is known
                    # overlap[rect_j.id] *= overlap_calculation(min_y, max_y, min_x, max_x,
                    #                                           x_cell_obj=rect_j.per_dimension_distribution[predicate.column_ids[1]],
                    #                                           y_cell_obj=bucket1.per_dimension_distribution[predicate.column_ids[0]])

                    # distribution of columns in cells is unknown
                    overlap[rect_j.id] *= overlap_calculation(min_y, max_y, min_x, max_x)
    total_card += (overlap * buckets2_card).sum()

    return total_card

def overlap_calculation(x_min, x_max, y_min, y_max, type = 2, x_cell_obj=None, y_cell_obj=None):
    # type 1 is integral, type 2 is sampling
    if type == 1:
        return overlap_percentage_integral(x_min, x_max, y_min, y_max)
    elif type == 2:
        return overlap_percentage_estimate_proportion(x_min, x_max, y_min, y_max, 50, x_cell_obj=x_cell_obj, y_cell_obj=y_cell_obj)
    else:
        print("Not implemented")
        exit(1)

# Define the joint probability density function
def joint_pdf(x, y, x_min, x_max, y_min, y_max):
    """
    Joint pdf for uniform distribution.
    :return:
    """
    if (x >= x_min and x <= x_max) and (y >= y_min and y <= y_max):
        if x_max == x_min and y_max == y_min:
            return 1
        elif x_max == x_min:
            return 1 / (y_max - y_min)
        elif y_max == y_min:
            return 1 / (x_max - x_min)

        return 1 / ((y_max - y_min) * (x_max - x_min))
    else:
        return 0


def overlap_percentage_integral(x_min, x_max, y_min, y_max):
    """
    Calculating the overlap by integrating over the region below x and y.
    :param x_min: lower boundary of range for x
    :param x_max: upper boundary of range for x
    :param y_min: lower boundary of range for y
    :param y_max: upper boundary of range for x
    :return:
    """
    result1, _ = integrate.dblquad(joint_pdf, max(x_min, y_min), min(x_max, y_max), max(x_min, y_min), lambda y: y,
                                   args=(x_min, x_max, y_min, y_max))

    result2, _ = integrate.dblquad(joint_pdf, x_max, y_max, x_min, x_max, args=(x_min, x_max, y_min, y_max))

    result3, _ = integrate.dblquad(joint_pdf, y_min, min(y_max, x_max), x_min, max(y_min, x_min), args=(x_min, x_max, y_min, y_max))

    return (result1 + result2 + result3)



def overlap_percentage_estimate_proportion(x_min, x_max, y_min, y_max, num_samples=1000, x_cell_obj=None, y_cell_obj=None):
    """
    Estimation based on samples.
    :param x_min: lower boundary of range for x
    :param x_max: upper boundary of range for x
    :param y_min: lower boundary of range for y
    :param y_max: upper boundary of range for x
    :param num_samples: number of samples used for estimation
    :param x_cell_obj: will not be None if we know the distribution of the column
    :param y_cell_obj: will not be None if we know the distribution of the column
    :return:
    """
    # Randomly sample points from each range
    if x_cell_obj is None:
        """if cell distribution is unknown assume uniform"""
        x_samples = np.random.uniform(x_min, x_max, num_samples)
    else:
        """if the cell distribution is known"""
        x_samples = x_cell_obj.sample_data(num_samples)

    if y_cell_obj is None:
        """if cell distribution is unknown assume uniform"""
        y_samples = np.random.uniform(y_min, y_max, num_samples)
    else:
        """if the cell distribution is known"""
        y_samples = y_cell_obj.sample_data(num_samples)

    # samples that satisfy the condition
    count_smaller = np.sum(x_samples < y_samples)
    # Estimate the proportion
    return count_smaller / num_samples

def parallel_execution(chunk_size, max_workers, lbs, rbs, rps):
    buckets2_card = np.array([])
    for i, b in enumerate(rbs):
        b.id = i
        buckets2_card = np.append(buckets2_card, b.num_points)

    rbs = np.array(rbs)
    rps = np.array(rps)
    # rps = np.flip(rps)
    with Pool(processes=max_workers) as pool:
        func = partial(process_range, rbs, rps, buckets2_card)
        results = pool.map(func, lbs)

    return sum(results)