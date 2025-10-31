from .main import *
import os

if __name__ == "__main__":
    poe_cwd = os.getenv("POE_CWD")
    if poe_cwd:
        os.chdir(poe_cwd)
    main()
