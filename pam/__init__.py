from .dataframe import DataFrame
from .series import Series
from .other_stuff import nan

# def clean_slices(phase, info):
#     if phase == "stop":
#         return
#     obj_dict = {}
#     data_dict = {}
#     for obj in gc.get_objects():
#         if isinstance(obj, (Series, DataFrame)):
#             d_id = id(obj.data)
#             data_dict[d_id] = obj.data
#             try:
#                 obj_dict[d_id].append(obj)
#             except:
#                 obj_dict[d_id] = [obj]
#
#     # check if the series is part of a dataframe. if so, skip it.
#
#     for d_id, objs in obj_dict.items():
#         # find minimum start or maximum stop
#         start = objs[0].view.start
#         stop = objs[0].view.stop
#         for obj in objs[1:]:
#             # early stopping if both are None. Data is still in use somewhere
#             if start is None and stop is None:
#                 break
#             if (start is not None) and (
#                 (obj.view.start is None) or obj.view.start < start
#             ):
#                 start = obj.view.start
#             if stop is not None and ((obj.view.stop is None) or obj.view.stop > stop):
#                 stop = obj.view.stop
#         # if both are None, don't do anything. No trimming necessary
#         if start is None and stop is None:
#             continue
#         if start is None:
#             start = 0
#         # adjust the data and view for each by trimming data and subtracting the start
#         data_dict[d_id][:] = data_dict[d_id][start:stop]
#         for obj in objs:
#             new_start = new_stop = None
#             if obj.view.start is not None:
#                 new_start = obj.view.start - start
#             if obj.view.stop is not None:
#                 new_stop = obj.view.stop - start
#             obj.view = slice(new_start, new_stop)
#             obj.index[:] = obj.index[start:stop]
