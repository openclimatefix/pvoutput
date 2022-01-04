"""
Generate gridded lat/lon coordinates that can be used for fixed radius searches across a region.

Provides both an importable method and a CLI.

.. code:: console

    $ python app.py -h

- Jamie Taylor <jamie.taylor@sheffield.ac.uk>
- First Authored: 2021-11-16
"""
import argparse
import logging
import os
import sys
from typing import Optional

from pvoutput.grid_search.grid_search import GridSearch
from pvoutput.grid_search.natural_earth import NaturalEarth


def parse_options():
    """Parse command line options."""
    parser = argparse.ArgumentParser(
        description=("This is a command line interface (CLI) for " "the grid_search module."),
        epilog="Jamie Taylor, 2021-11-16",
    )
    parser.add_argument(
        "--bbox",
        metavar="<min_lat,min_lon,max_lat,max_lon>",
        dest="bbox",
        action="store",
        type=str,
        required=False,
        default=None,
        help="Specify a bounding box to search. Can be used in conjunction with " "--countries",
    )
    parser.add_argument(
        "--countries",
        metavar="<country>[,<country>[,...]]",
        dest="countries",
        action="store",
        type=str,
        required=False,
        default=None,
        help="Specify a list of countries, searching only grid points that fall "
        "within these countries' boundaries. Specify one or more countries, "
        "separated by commas (default is all). Country names must match those "
        "used in the Natural Earth dataset (HINT: run this code with the "
        "--list-countries option to list them). This option can be used in "
        "conjunction with --bbox, in which case the search will only include "
        "grid points within both the bounding box and the countries list.",
    )
    parser.add_argument(
        "--radial-clip",
        metavar="<lat,lon,radius_km>",
        dest="radial_clip",
        action="store",
        type=str,
        required=False,
        default=None,
        help="Specify a radius to clip to. Can be used in conjunction with --bbox "
        "and --countries. Pass the latitude, longitude and radius as a "
        "comma-separated string. Radius should be in km.",
    )
    parser.add_argument(
        "--list-countries",
        dest="list_countries",
        action="store_true",
        required=False,
        help="List the country names that can be used for the " "--countries option.",
    )
    parser.add_argument(
        "--buffer",
        metavar="<buffer>",
        dest="buffer",
        action="store",
        type=float,
        required=False,
        default=0.0,
        help="Specify a buffer/tolerance for including grid points i.e. include "
        "grid points that fall within <buffer> kilometers of the target "
        "boundary. Default is 0km.",
    )
    parser.add_argument(
        "--search-radius",
        metavar="<radius>",
        dest="search_radius",
        action="store",
        type=float,
        required=False,
        default=25.0,
        help="Specify the radial search limit around each grid point in " "kilometers.",
    )
    parser.add_argument(
        "--local-crs-epsg",
        metavar="<EPSG>",
        dest="local_crs_epsg",
        action="store",
        type=int,
        required=False,
        default=4087,
        help="Optionally provide the EPSG code of a local co-ordinate Reference "
        "System (CRS) for improved accuracy. e.g. set to 27700 (OSGB36 / "
        "British National Grid) if searching the British Isles.",
    )
    parser.add_argument(
        "--cache-dir",
        metavar="</path/to/dir>",
        dest="cache_dir",
        action="store",
        type=str,
        required=False,
        default=None,
        help="Specify a directory to use for caching downloaded boundary files.",
    )
    parser.add_argument(
        "--show",
        dest="show",
        action="store_true",
        required=False,
        help="Set this flag to show a plot of the grid.",
    )
    parser.add_argument(
        "-o",
        "--outfile",
        metavar="<path>",
        dest="outfile",
        action="store",
        type=str,
        required=False,
        help="Specify a filename to save the grid to.",
    )
    options = parser.parse_args()

    def handle_options(options):
        """Validate command line args and pre-process where necessary."""
        if options.bbox is not None:
            options.bbox = list(map(lambda x: float(x.strip()), options.bbox.split(",")))
        if options.radial_clip is not None:
            options.radial_clip = list(
                map(lambda x: float(x.strip()), options.radial_clip.split(","))
            )
        if options.cache_dir is not None:
            if not os.path.isdir(options.cache_dir):
                logging.error(f"The cache_dir '{options.cache_dir}' does not exist.")
                raise ValueError(f"The cache_dir '{options.cache_dir}' does not exist.")
        if options.countries:
            options.countries = list(map(lambda x: str(x.strip()), options.countries.split(",")))
            earth = NaturalEarth(options.cache_dir)
            _, countries = earth.get_hires_world_boundaries()
            for country in options.countries:
                if country not in countries:
                    logging.error(f"The country '{country}' is invalid.")
                    raise ValueError(f"The country '{country}' is invalid.")
        if options.outfile is not None and os.path.exists(options.outfile):
            check = query_yes_no(
                f"The output file '{options.outfile}' already exists, results "
                "will be overwritten, do you want to continue?",
                "no",
            )
            if check is False:
                print("Quitting...")
                sys.exit(0)
        return options

    return handle_options(options)


def main():
    options = parse_options()
    grd = GridSearch(cache_dir=options.cache_dir)
    if options.list_countries:
        grd.nat_earth.list_countries()
        sys.exit()
    grd.generate_grid(
        bbox=options.bbox,
        countries=options.countries,
        radial_clip=options.radial_clip,
        buffer=options.buffer,
        search_radius=options.search_radius,
        local_crs_epsg=options.local_crs_epsg,
        save_to=options.outfile,
        show=options.show,
    )


if __name__ == "__main__":
    fmt = "%(asctime)s [%(levelname)s] - %(message)s (%(filename)s:%(funcName)s)"
    datefmt = "%Y-%m-%dT%H:%M:%SZ"
    logging.basicConfig(format=fmt, datefmt=datefmt, level=os.environ.get("LOGLEVEL", "INFO"))
    main()


def query_yes_no(question: str, default: Optional[str] = "yes") -> bool:
    """Ask a yes/no question via input() and return the answer as boolean.

    Args:
        question:
            The question presented to the user.
        default:
            The presumed answer if the user just hits <Enter>. It must be "yes" (the default), "no"
            or None (meaning an answer is required of the user).

    Returns:
        Return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)
    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")
