"""
Contains tools for processing files (reading and writing csv and json files).
"""

from __future__ import with_statement, division

from bandicoot_dev.helper.tools import OrderedDict

from bandicoot_dev.core import User, Record, Position
from bandicoot_dev.helper.tools import percent_records_missing_location, warning_str
from bandicoot_dev.utils import flatten

from datetime import datetime
from json import dumps
from collections import Counter
import csv
import os


def to_csv(objects, filename, digits=5):
    """
    Export the flatten indicators of one or several users to CSV.

    Parameters
    ----------
    objects : list
        List of objects to be exported.
    filename : string
        File to export to.
    digits : int
        Precision of floats.

    Examples
    --------
    This function can be use to export the results of :meth`bandicoot.utils.all`.
    >>> U_1 = bc.User()
    >>> U_2 = bc.User()
    >>> bc.to_csv([bc.utils.all(U_1), bc.utils.all(U_2)], 'results_1_2.csv')

    If you only have one object, you can simply pass it as argument:
    >>> bc.to_csv(bc.utils.all(U_1), 'results_1.csv')
    """

    if not isinstance(objects, list):
        objects = [objects]

    data = [flatten(obj) for obj in objects]
    all_keys = [d for datum in data for d in datum.keys()]
    field_names = sorted(set(all_keys), key=lambda x: all_keys.index(x))

    with open(filename, 'wb') as f:
        w = csv.writer(f)
        w.writerow(field_names)

        def make_repr(item):
            if item is None:
                return None
            elif isinstance(item, float):
                return repr(round(item, digits))
            else:
                return str(item)

        for row in data:
            row = dict((k, make_repr(v)) for k, v in row.items())
            w.writerow([make_repr(row.get(k, None)) for k in field_names])

    print "Successfully exported %d object(s) to %s" % (len(objects), filename)


def to_json(objects, filename):
    """
    Export the indicators of one or several users to JSON.

    Parameters
    ----------
    objects : list
        List of objects to be exported.
    filename : string
        File to export to.

    Examples
    --------
    This function can be use to export the results of :meth`bandicoot.utils.all`.
    >>> U_1 = bc.User()
    >>> U_2 = bc.User()
    >>> bc.to_json([bc.utils.all(U_1), bc.utils.all(U_2)], 'results_1_2.json')

    If you only have one object, you can simply pass it as argument:
    >>> bc.to_json(bc.utils.all(U_1), 'results_1.json')
    """

    if not isinstance(objects, list):
        objects = [objects]

    obj_dict = {obj['name']: obj for obj in objects}

    with open(filename, 'wb') as f:
        f.write(dumps(obj_dict, indent=4, separators=(',', ': ')))
    print "Successfully exported %d object(s) to %s" % (len(objects), filename)


def _tryto(function, argument):
    try:
        return function(argument)
    except Exception as ex:
        return ex


def _parse_record(data):
    def _map_duration(s):
        return int(s) if s != '' else None

    return Record(interaction=data['interaction'],
                  direction=data['direction'],
                  correspondent_id=data['correspondent_id'],
                  datetime=_tryto(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"), data['datetime']),
                  duration=_tryto(_map_duration, data['duration'])
                  )


def filter_record(records):
    """
    Filter records and remove items with missing or inconsistent fields

    Parameters
    ----------

    records : list
        A list of Record objects

    Returns
    -------

    records, ignored : (Record list, dict)
        A tuple of filtered records, and a dictionary counting the missings fields

    """

    scheme = {
        'interaction': lambda r: r.interaction in ['call', 'text', 'physical', ''],
        'direction': lambda r: r.direction in ['in', 'out', ''],
        'correspondent_id': lambda r: r.correspondent_id is not None,
        'datetime': lambda r: isinstance(r.datetime, datetime),
        'duration': lambda r: isinstance(r.duration, (int, float)) if r.interaction == 'call' else True,
    }

    ignored = OrderedDict([
        ('all', 0),
        ('interaction', 0),
        ('direction', 0),
        ('correspondent_id', 0),
        ('datetime', 0),
        ('duration', 0),
    ])

    bad_records = []

    def _filter(records):
        global removed
        for r in records:
            valid = True
            for key, test in scheme.iteritems():
                if not test(r):
                    ignored[key] += 1
                    bad_records.append(r)
                    valid = False  # Not breaking, we count all fields with errors

            if valid is True:
                yield r
            else:
                ignored['all'] += 1

    return list(_filter(records)), ignored, bad_records


def load(name, records, attributes=None, attributes_path=None, 
         describe=False, warnings=False):
    """
    Creates a new user. This function is used by read_csv, read_orange,
    and read_telenor. If you want to implement your own reader function, we advise you to use the load() function

    `load` will output warnings on the standard output if some records are missing a position.

    Parameters
    ----------

    name : str
        The name of the user. It is stored in User.name and is useful when
        exporting metrics about multiple users.

    records: list
        A list or a generator of Record objects.

    attributes : dict
        A (key,value) dictionary of attributes for the current user

    describe : boolean
        If describe is True, it will print a description of the loaded user
        to the standard output. Defaults to false.

    warnings : boolean, default True
        If warnings is equal to False, the function will not output the
        warnings on the standard output.


    For instance:

    .. code-block:: python

       >>> records = [Record(...),...]
       >>> attributes = {'age': 60}
       >>> load("Frodo", records, attributes)

    will returns a new User object.


    """

    user = User()
    user.name = name
    user.attributes_path = attributes_path

    user.records, ignored, bad_records = filter_record(records)

    if ignored['all'] != 0:
        if warnings:
            print warning_str("Warning: %d record(s) were removed due to missing or incomplete fields." % ignored['all'])
        for k in ignored.keys():
            if k != 'all' and ignored[k] != 0 and warnings:
                print warning_str(" " * 9 + "%s: %i record(s) with incomplete values" % (k, ignored[k]))

    user.ignored_records = dict(ignored)

    if attributes is not None:
        user.attributes = attributes

    percent_missing = percent_records_missing_location(user)
    if percent_missing > 0 and warnings:
        print warning_str("Warning: {0:.2%} of the records are missing a location.".format(percent_missing))

    sorted_min_records = sorted(set(user.records), key=lambda r: r.datetime)
    num_dup = len(user.records) - len(sorted_min_records)
    if num_dup > 0 and warnings:
        print warning_str("Warning: {0:d} duplicated record(s) were removed.".format(num_dup))
        user.records = sorted_min_records

    if describe is True:
        user.describe()

    return user, bad_records


def _read_network(user, records_path, attributes_path, read_function, extension=".csv"):
    connections = {}
    correspondents = Counter([record.correspondent_id for record in user.records])

    # Try to load all the possible correspondent files
    for c_id, count in sorted(correspondents.items()):
        correspondent_file = os.path.join(records_path, c_id + extension)
        if os.path.exists(correspondent_file):
            connections[c_id] = read_function(c_id, records_path, attributes_path, describe=False, network=False, warnings=False)
        else:
            connections[c_id] = None

    def _is_consistent(record):
        if record.correspondent_id == user.name:
            correspondent = user
        elif record.correspondent_id in connections:
            correspondent = connections[record.correspondent_id]
        else:
            return True  # consistent by default

        return True if correspondent is None else record.has_match(correspondent.records)

    def all_user_iter():
        if user.name not in connections:
            yield user

        for u in connections.values():
            if u is not None:
                yield u

    # Filter records and count total number of records before/after
    num_total_records = sum(len(u.records) for u in all_user_iter())
    for u in all_user_iter():
        u.records = filter(_is_consistent, u.records)
    num_total_records_filtered = sum(len(u.records) for u in all_user_iter())

    # Report non reciprocated records
    num_inconsistent_records = num_total_records - num_total_records_filtered
    if num_inconsistent_records > 0:
        percent_inconsistent = num_inconsistent_records / num_total_records
        print warning_str('Warning: {} records ({:.2%}) for all users in the network were not reciprocated. They have been removed.'.format(num_inconsistent_records, percent_inconsistent))

    # Return the network dictionary sorted by key
    return OrderedDict(sorted(connections.items(), key=lambda t: t[0]))


def read_csv(user_id, records_path, attributes_path=None, network=False, describe=True, warnings=True, errors=False):
    """
    Load user records from a CSV file.

    Parameters
    ----------

    user_id : str
        ID of the user (filename)

    records_path : str
        Path of the directory all the user files.

    attributes_path : str, optional
        Path of the directory containing attributes files (``key, value`` CSV file).
        Attributes can for instance be variables such as like, age, or gender.
        Attributes can be helpful to compute specific metrics.

    network : bool, optional
        If network is True, bandicoot loads the network of the user's correspondants from the same path. Defaults to False.

    describe : boolean
        If describe is True, it will print a description of the loaded user to the standard output.

    errors : boolean
        If errors is True, returns a tuple (user, errors), where user is the user object and errors are the records which could not
        be loaded.


    Examples
    --------

    >>> user = bandicoot.read_csv('sample_records', '.')
    >>> print len(user.records)
    10

    >>> user = bandicoot.read_csv('sample_records', '.', None, 'sample_attributes.csv')
    >>> print user.attributes['age']
    25

    Notes
    -----
    - The csv files can be single, or double quoted if needed.
    - Empty cells are filled with ``None``. For example, if the column
      ``duration`` is empty for one record, its value will be ``None``.
      Other values such as ``"N/A"``, ``"None"``, ``"null"`` will be
      considered as a text.
    """

    user_records = os.path.join(records_path, user_id + '.csv')
    with open(user_records, 'rb') as csv_file:
        reader = csv.DictReader(csv_file)
        records = map(_parse_record, reader)

    # Optional
    attributes = None
    if attributes_path is not None:
        user_attributes = os.path.join(attributes_path, user_id + '.csv')
        try:
            with open(user_attributes, 'rb') as csv_file:
                reader = csv.DictReader(csv_file)
                attributes = dict((d['key'], d['value']) for d in reader)
        except IOError:
            attributes = None

    user, bad_records = load(user_id, records, attributes, 
                             attributes_path=attributes_path, describe=False, warnings=warnings)

    # Loads the network
    if network is True:
        user.network = _read_network(user, records_path, attributes_path, read_csv)
        user.recompute_missing_neighbors()

    if describe:
        user.describe()

    if errors:
        return user, bad_records
    return user