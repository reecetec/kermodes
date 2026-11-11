from IPython.core.interactiveshell import InteractiveShell


def test_rerunning_a_cell_replaces_the_table_it_built():
    shell = InteractiveShell.instance()
    shell.run_cell(
        "import ursid as us\n"
        "s = us.local(manual=True)\n"
        "src = s.input_table({'sym': us.string(), 'size': us.int64()})"
    )
    for _ in range(3):
        # Displayed, so IPython's output history sees each table too.
        shell.run_cell("latest = src.last_by('sym')\nlatest", store_history=True)
    session = shell.user_ns["s"]
    assert session.table_count == 2, "the input and the newest last_by"

    # The history still reaches the table it shows while a name holds it, but doesn't keep it.
    assert shell.run_cell("len(Out[max(Out)])").result == 0
    shell.run_cell("del latest")
    assert session.table_count == 1
