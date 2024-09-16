# import os
# import ops_logging
#
# GREY = "\x1b[38;20m"
# YELLOW = "\x1b[33;20m"
# RED = "\x1b[31;20m"
# BOLD_RED = "\x1b[31;1m"
# RESET = "\x1b[0m"
#
# log_dir = "logs"
# log_filepath = os.path.join(log_dir, "running_logs.log")
# os.makedirs(log_dir, exist_ok=True)
#
# # Notice "#color" and "#reset" tags inside format
# format = "%(asctime)s: %(name)s:  #color%(levelname)s#reset: %(filename)s: %(message)s"
#
# colors = {
#     ops_logging.DEBUG: GREY,
#     ops_logging.INFO: GREY,
#     ops_logging.WARNING: YELLOW,
#     ops_logging.ERROR: RED,
#     ops_logging.CRITICAL: BOLD_RED,
# }
#
#
# class ColorFormatter(ops_logging.Formatter):
#     def __init__(self, *args, colors, **kwargs):
#         super().__init__(*args, **kwargs)
#
#         replace_tags = lambda level: (self._style._fmt
#                                       .replace("#color", colors.get(level, ""))
#                                       .replace("#reset", RESET))
#         levels = set(ops_logging.getLevelNamesMapping().values())
#         self._fmts = {level: replace_tags(level) for level in levels}
#
#     def format(self, record):
#         self._style._fmt = self._fmts.get(record.levelno)
#         return super().format(record)
#
#
# logger = ops_logging.getLogger("mlDatabricksLogger")
# logger.setLevel(ops_logging.INFO)
# sh = ops_logging.StreamHandler()
# fh = ops_logging.FileHandler(log_filepath)
# sh.setFormatter(ColorFormatter(fmt=format, colors=colors))
# logger.addHandler(sh)
# logger.addHandler(fh)
