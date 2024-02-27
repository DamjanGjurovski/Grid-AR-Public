import numpy as np
from fitter import get_common_distributions
from .distribution_identifier import CustomScipyDist
import scipy.stats as st

class GridCell:
    """
    A class that represents one cell of the gird.
    """
    def __init__(self, num_dimension):
        self.cell_data = list()
        self.max_val_per_dim = [-1] * num_dimension
        self.min_val_per_dim = [1000] * num_dimension
        self.num_data_points = 0
        self.len_samples = 0
        self.range_dimensions = []
        self.cell_volume = 1.0
        self.per_dimension_distribution = list()

    def add_tuple(self, data_point):
        # store the data point
        self.cell_data.append(data_point)

        # update the number of points for the cell
        self.num_data_points += 1
        for dim in range(len(data_point)):
            # update the maximal value per dimension
            self.max_val_per_dim[dim] = max(self.max_val_per_dim[dim], data_point[dim])
            # update the minimal value per dimension
            self.min_val_per_dim[dim] = min(self.min_val_per_dim[dim], data_point[dim])

    def get_best_distribution(self, data):
        dist_names = get_common_distributions()
        dist_results = []
        params = {}
        for dist_name in dist_names:
            dist = getattr(st, dist_name)
            param = dist.fit(data)
            params[dist_name] = param
            # Applying the Kolmogorov-Smirnov test
            D, p = st.kstest(data, dist_name, args=param)

            dist_results.append((dist_name, p))

        # select the best fitted distribution
        best_dist, best_p = (max(dist_results, key=lambda item: item[1]))
        return best_dist, best_p, params[best_dist]

    def identify_per_dimension_distribution(self):
        """
        Identifying the distribution from the data in the cell.
        :return:
        """
        tmp_cell_data = np.array(self.cell_data)
        for dimension in range(len(self.max_val_per_dim)):
            data_for_dimension = tmp_cell_data[:, dimension]

            best_dist, best_p, params = self.get_best_distribution(data_for_dimension)
            """fitting with option 2"""

            tmp_list = dict()
            tmp_list[0] = best_dist
            tmp_list[1] = params
            self.per_dimension_distribution.append(CustomScipyDist(tmp_list))


    def post_process_cell(self):
        """
        Once the data has be added to the cell, compute the volume and remove the actual tuples.
        """
        self.range_dimensions = range(len(self.max_val_per_dim))

        """for the grid cell compute the volume (area if 2 dimensions)"""
        self.cell_volume = self.compute_volume(self.min_val_per_dim, self.max_val_per_dim)

        """before we delete the data identify the distribution for every dimension in the cell"""
        """if not called, uniform distribution per column is assumed"""
        # self.identify_per_dimension_distribution()

        self.cell_data = None

    def data_point_satisfies_query_boundary(self, data_point, query_min_points, query_max_points):
        """
        Checks if the data point is good for the query boundaries in all dimensions.
        If for any dimension the data point is lower than the minimal boundary
        or higher than the maximal one return false.
        SO ASSUMPTION IS THAT THE SIGN IS ALWAYS EQUAL IN THE QUERY
        :param data_point: a point from the cell
        :param query_min_points: the minimal values per dimension from the query
        :param query_max_points: the maximal values per dimension from the query
        :return: True if the data point satisfies the query otherwise false
        """

        for dim in self.range_dimensions:
            if data_point[dim] < query_min_points[dim] or data_point[dim] > query_max_points[dim]:

                return False
        return True

    def compute_volume(self, min_points, max_points):
        """
        Computes the volume/area between the two given points representing the
        minimal and maximal point of the area in question.
        Ex. For 3 dimensions the volume is: dim_1_length * dim_2_length * dim_3_length
        :param min_points: the value per dimension for the minimal point
        :param max_points: the value per dimension for the maximal point
        :return: volume
        """
        cell_volume = 1
        for dim, dim_min in enumerate(min_points):
            dim_max = max_points[dim]

            if (dim_max - dim_min) < 0:
                return 0
            cell_volume = (cell_volume * (dim_max - dim_min) if (dim_max - dim_min) != 0 else cell_volume)
        return cell_volume

    def intersection_of_grid_cell_and_query(self, query_min_points, query_max_points):
        """
        Method for computing the intersecting area between a query and the grid cell.
        In other words, it computes the part of the grid that is captured by the query
        :param query_min_points: the value per dimension for the minimal point of the query
        :param query_max_points: the value per dimension for the maximal point of the query
        :return: intersect_min_point AND intersect_max_point
        """
        # for the minimal point get the maximums per dimension
        intersect_min_point = np.maximum(query_min_points, self.min_val_per_dim)
        # for the maximal point get the minimums per dimension
        intersect_max_point = np.minimum(query_max_points, self.max_val_per_dim)
        return intersect_min_point, intersect_max_point

    def estimation_function(self, query_min_points, query_max_points):
        # compute the intersecting area between the query and the grid cell, i.e.,
        # the part of the grid cell that is captured by the query
        intersect_min, intersect_max = self.intersection_of_grid_cell_and_query(query_min_points, query_max_points)
        # compute the volume of the intersecting part between the query and the grid
        intersection_volume = self.compute_volume(intersect_min, intersect_max) * 1.0

        # estimate the number of tuples that satisfy the query for this grid cell
        estimation = (intersection_volume / self.cell_volume) * self.num_data_points
        return estimation

    def single_table_estimation(self, query_min_points, query_max_points):
        # get the estimated number of tuples for the grid cell that satisfy the query
        query_estimation_for_cell = self.estimation_function(query_min_points, query_max_points)

        return (1. * query_estimation_for_cell), self.num_data_points

    def print_grid_cell(self, cell_index):
        print(f'Grid {cell_index} has {self.num_data_points} tuples')
        print(f'\t minimal values per dimension: \n \t\t {self.min_val_per_dim}')
        print(f'\t maximal values per dimension: \n \t\t {self.max_val_per_dim}')
        print(f'\t data points for grid: \n \t\t {self.cell_data}')
        print(f'\t data point ids for grid: \n \t\t {self.cell_data_tuple_ids}')
        print()
