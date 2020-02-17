from click import command, option, echo

@command()
@option('--redo', '-r', is_flag=True, default=False)
@option('--stop-on-error', is_flag=True, default=False)
@option('--verbose', '-v', is_flag=True, default=False)
@option('--show-data', '-S', is_flag=True, default=False)
def cli(redo=False, stop_on_error=False, verbose=False, show_data=False):
    echo("Hello, world!")
