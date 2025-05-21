 
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(prog='gridflow', description='GridFlow CLI and GUI for climate data processing')
    parser.add_argument('--gui', action='store_true', help='Launch the GUI')
    args, unknown = parser.parse_known_args()

    if args.gui:
        from gui.main import main as gui_main
        gui_main()
    else:
        from gridflow.__main__ import main as cli_main
        sys.argv = sys.argv[:1] + unknown  # Pass remaining args to CLI
        cli_main()

if __name__ == '__main__':
    main()