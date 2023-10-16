import os


def set_github_action_output(name, value):
    value = str(value).replace('"', "''")
    os.system(f"echo \"{name}={value}\" >> $GITHUB_OUTPUT")
