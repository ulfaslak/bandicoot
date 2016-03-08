from __future__ import division

import datetime
from collections import Counter
from bandicoot_dev.helper.tools import Colors
from bandicoot_dev.helper.group import _binning
import bandicoot_dev as bc


class Record(object):
    """
    Data structure storing a call detail record.

    Attributes
    ----------
    interaction : str, 'text' or 'call'
        The type of interaction (text message or call).
    direction : str, 'in' or 'out'
        The direction of the record (incoming or outgoing).
    correspondent_id : str
        A unique identifier for the corresponding contact
    datetime : datetime
        The exact date and time of the interaction
    call_duration : int or None
        Durations of the call in seconds. None if the record is a text message.
    position : Position
        The geographic position of the user at the time of the interaction.
    """

    __slots__ = ['interaction', 'direction', 'correspondent_id', 'datetime', 'call_duration', 'position']

    def __init__(self, interaction, direction, correspondent_id, datetime, call_duration, position):
        self.interaction = interaction
        self.direction = direction
        self.correspondent_id = correspondent_id
        self.datetime = datetime
        self.call_duration = call_duration
        self.position = position

    def __repr__(self):
        return "Record(" + ", ".join(map(lambda x: "%s=%r" % (x, getattr(self, x)), self.__slots__)) + ")"

    def __eq__(self, other):
        if isinstance(other, self.__class__) and self.__slots__ == other.__slots__:
            return all(getattr(self, attr) == getattr(other, attr) for attr in self.__slots__)
        return False

    def __hash__(self):
        return hash(self.__repr__())

    def matches(self, other):
        """
        Returns true if two records 'match' - that is, they correspond to the same event from two perspectives.
        """
        return self.interaction == other.interaction and \
            self.direction != other.direction and \
            self.call_duration == other.call_duration and \
            abs((self.datetime - other.datetime).total_seconds()) < 30

    def all_matches(self, iterable):
        return filter(self.matches, iterable)

    def has_match(self, iterable):
        return len(self.all_matches(iterable)) > 0


class Position(object):
    """
    Data structure storing a generic location. Can be instantiated with either an
    antenna or a gps location. Printing out the position will show which was used
    to instantiate it.
    """

    def __init__(self, antenna=None, location=None):
        self.antenna = antenna
        self.location = location

    def _get_location(self, user):
        if self.location:
            return self.location
        elif self.antenna:
            return user.antennas.get(self.antenna)
        else:
            return None

    def type(self):
        if self.antenna:
            return 'antenna'
        else:
            return 'gps'

    def __repr__(self):
        if self.antenna and self.location:
            return "Position(antenna=%s, location=%s)" % (self.antenna, self.location)
        if self.antenna:
            return "Position(antenna=%s)" % self.antenna
        if self.location:
            return "Position(location=(%s, %s))" % self.location

        return "Position()"

    def __eq__(self, other):
        if not isinstance(other, Position):
            return False
        if self.antenna and other.antenna:
            return self.antenna == other.antenna
        if self.location and other.location:
            return self.location == other.location

        if not self.location and not self.antenna and not other.location and not other.antenna:
            return True

        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__repr__())


class User(object):
    """
    Data structure storing all the call, text or mobility records of the user.
    """

    def __init__(self):
        self._records = []
        self._antennas = {}

        self.name = None
        self.antennas_path = None
        self.attributes_path = None

        self.start_time = None
        self.end_time = None
        self.night_start = datetime.time(19)
        self.night_end = datetime.time(7)
        self.weekend = [6, 7]  # Saturday, Sunday by default

        self.home = None
        self.has_text = False
        self.has_call = False
        self.has_antennas = False
        self.attributes = {}
        self.ignored_records = None

        self.percent_outofnetwork_calls = 0
        self.percent_outofnetwork_texts = 0
        self.percent_outofnetwork_contacts = 0
        self.percent_outofnetwork_call_durations = 0

        self.network = {}

    @property
    def antennas(self):
        """
        The purpose of this is to hook into assignments to the
        user's antenna dictionary, and update records' location
        based on the new value.
        """
        return self._antennas

    @antennas.setter
    def antennas(self, input_):
        self._antennas = input_
        self.has_antennas = len(input_) > 0
        for r in self._records:
            if r.position.antenna in self._antennas:
                r.position.location = self._antennas[r.position.antenna]
            else:
                r.position.location = None

    @property
    def records(self):
        """
        Can be used to get or set the list of user's records.

        If the records are modified, the start and end time as well as the
        infered house properties of the object User are recomputed.
        """
        return self._records

    @records.setter
    def records(self, input):
        self._records = sorted(input, key=lambda r: r.datetime)
        if len(self._records) > 0:
            self.start_time = self._records[0].datetime
            self.end_time = self._records[-1].datetime

        # Reset all the states
        self.has_call = False
        self.has_text = False
        self.has_antennas = False

        for r in self._records:
            if r.interaction == 'text':
                self.has_text = True
            elif r.interaction == 'call':
                self.has_call = True

            if r.position.type() == 'antenna':
                self.has_antennas = True

        self.recompute_home()

    def recompute_missing_neighbors(self):
        """
        Recomputes statistics for missing users of the current user's
        network:

        - ``User.percent_outofnetwork_calls``
        - ``User.percent_outofnetwork_texts``
        - ``User.percent_outofnetwork_contacts``
        - ``User.percent_outofnetwork_call_durations``

        This function is automatically called from :meth:`~bandicoot.io.read_csv`
        when loading a network user.
        """

        oon_records = [r for r in self.records if self.network.get(r.correspondent_id, None) is None]
        num_oon_calls = len([r for r in oon_records if r.interaction == 'call'])
        num_oon_texts = len([r for r in oon_records if r.interaction == 'text'])
        num_oon_neighbors = len(set(x.correspondent_id for x in oon_records))
        oon_call_durations = sum([r.call_duration for r in oon_records if r.interaction == 'call'])

        num_calls = len([r for r in self.records if r.interaction == 'call'])
        num_texts = len([r for r in self.records if r.interaction == 'text'])
        total_neighbors = len(set(x.correspondent_id for x in self.records))
        total_call_durations = sum([r.call_duration for r in self.records if r.interaction == 'call'])

        def _safe_div(a, b, default):
            return a / b if b != 0 else default

        # We set the percentage at 0 if no event occurs
        self.percent_outofnetwork_calls = _safe_div(num_oon_calls, num_calls, 0)
        self.percent_outofnetwork_texts = _safe_div(num_oon_texts, num_texts, 0)
        self.percent_outofnetwork_contacts = _safe_div(num_oon_neighbors, total_neighbors, 0)
        self.percent_outofnetwork_call_durations = _safe_div(oon_call_durations, total_call_durations, 0)

    def describe(self):
        """
        Generate a short description of the object.

        Examples
        --------
        >>> import bandicoot_dev as bc
        >>> user = bc.User()
        >>> user.records = bc.tests.generate_user.random_burst(5)
        >>> user.describe()
        [x] 5 records from 2014-01-01 10:41:00 to 2014-01-01 11:21:00
            5 contacts
        [x] 1 attribute

        Note
        ----
        The summary is directly sent to the standard output.
        """

        def format_int(name, n):
            if n == 0 or n == 1:
                return "%i %s" % (n, name[:-1])
            else:
                return "%i %s" % (n, name)

        empty_box = Colors.OKGREEN + '[ ]' + Colors.ENDC + ' '
        filled_box = Colors.OKGREEN + '[x]' + Colors.ENDC + ' '

        if self.start_time is None:
            print empty_box + "No records stored"
        else:
            print (filled_box + format_int("records", len(self.records)) +
                   " from %s to %s" % (self.start_time, self.end_time))

        nb_contacts = bc.individual.number_of_contacts(self, interaction='callandtext', groupby=None)
        nb_contacts = nb_contacts['allweek']['allday']['callandtext']
        if nb_contacts:
            print filled_box + format_int("contacts", nb_contacts)
        else:
            print empty_box + "No contacts"

        if self.has_attributes:
            print filled_box + format_int("attributes", len(self.attributes))
        else:
            print empty_box + "No attribute stored"

        if len(self.antennas) == 0:
            print empty_box + "No antenna stored"
        else:
            print filled_box + format_int("antennas", len(self.antennas))

        if self.has_home:
            print filled_box + "Has home"
        else:
            print empty_box + "No home"

        if self.has_text:
            print filled_box + "Has texts"
        else:
            print empty_box + "No texts"

        if self.has_call:
            print filled_box + "Has calls"
        else:
            print empty_box + "No calls"

        if self.has_network:
            print filled_box + "Has network"
        else:
            print empty_box + "No network"

    def recompute_home(self):
        """
        Return the antenna where the user spends most of his time at night.
        None is returned if there are no candidates for a home antenna
        """

        if self.night_start < self.night_end:
            night_filter = lambda r: self.night_end > r.datetime.time() > self.night_start
        else:
            night_filter = lambda r: not(self.night_end < r.datetime.time() < self.night_start)

        # Bin positions by chunks of 30 minutes
        candidates = list(_binning(filter(night_filter, self._records)))

        if len(candidates) == 0:
            self.home = None
        else:
            self.home = Counter(candidates).most_common()[0][0]

        return self.home

    @property
    def has_home(self):
        return self.home is not None

    @property
    def has_attributes(self):
        return len(self.attributes) != 0

    @property
    def has_network(self):
        return self.network != {}

    def set_home(self, new_home):
        """
        Sets the user's home. The argument can be a Position object or a
        tuple containing location data.
        """
        if type(new_home) is Position:
            self.home = new_home

        elif type(new_home) is tuple:
            self.home = Position(location=new_home)

        else:
            self.home = Position(antenna=new_home)
