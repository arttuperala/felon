import functools


class CommonOptions:
    """Decorator to apply common Click options and handle them in a consistent manner
    before the main command body.

    Implement by setting `options` to a sequence of `click.option` calls that establish
    all of the options that the command(s) should use and implementing `process()`.

    Inspiration from https://github.com/pallets/click/issues/108#issuecomment-255547347.
    """

    options = ()

    def __init__(self, func):
        for option in reversed(self.options):
            func = option(func)
        functools.update_wrapper(self, func)
        self.func = func

    def __call__(self, context, *args, **kwargs):
        self.process(context, kwargs)
        return self.func(context, *args, **kwargs)

    def process(self, context, kwargs: dict):
        """Implement the logic for processing the options into context.

        It is recommended to remove all of the processed option values by calling
        `kwargs.pop()` so that they're not passed onto the main command function.
        """
        raise NotImplementedError
