from __future__ import division


from bandicoot_dev.helper.group import grouping
from bandicoot_dev.helper.tools import summary_stats, entropy, pairwise
from collections import Counter, defaultdict

import math
import numpy as np
import datetime


@grouping(interaction=["screen"])
def interevent_time(records):
    """
    The interevent time between two records of the user.
    """
    inter_events = pairwise(r.datetime for r in records)
    inter = [(new - old).total_seconds() for old, new in inter_events]

    return np.mean(inter)

@grouping(interaction=["call", "text", "physical", "stop"])
def number_of_contacts(records, direction=None, more=1):
    """Number of contacts the user interacted with.

    Parameters
    ----------
    direction : str, optional
        Filters the records by their direction: ``None`` for all records,
        ``'in'`` for incoming, and ``'out'`` for outgoing.
    more : int, optional
        Counts only contacts with more than this number of interactions. Defaults to 0.
    """
    records = list(records)
    
    if hasattr(records[0], 'correspondent_id'):
        counter = Counter(
            r.correspondent_id for r in records
            if (hasattr(r, 'duration') and r.duration > 5)
            or not hasattr(r, 'duration')
        )
    else:
        counter = Counter(
            r.position for r in records 
        )

    return sum(1 for d in counter.values() if d > more)

@grouping(interaction=["call", "text", "physical", 'stop'])
def number_of_interactions(records, direction=None):
    """Total number of interactions.

    Parameters
    ----------
    direction : str, optional
        Filters the records by their direction: ``None`` for all records,
        ``'in'`` for incoming, and ``'out'`` for outgoing.
    """
    if direction is None:
        return len([r for r in records])
    else:
        return len([r for r in records if r.direction == direction])

@grouping(interaction=["call", "text", "physical", "stop"])
def entropy_per_contacts(records, normalize=True):
    """Entropy of the user's contacts. Time uncorrelated.

    Parameters
    ----------
    normalize: boolean, default is False
        Returns a normalized entropy between 0 and 1.
    """
    try:
        counter = Counter(r.correspondent_id for r in records)
    except AttributeError:
        counter = Counter(r.position for r in records)

    raw_entropy = entropy(counter.values())
    n = len(counter)
    if normalize and n > 1:
        return raw_entropy / np.log2(n)
    else:
        return raw_entropy

@grouping(interaction=["call", "text", "physical", "stop"])
def interactions_per_contact(records):
    """Number of interactions a user had with each of its contacts.

    Parameters
    ----------
    direction : str, optional
        Filters the records by their direction: ``None`` for all records,
        ``'in'`` for incoming, and ``'out'`` for outgoing.
    """
    
    try:
        contacts = [r.correspondent_id for r in records]
    except AttributeError:
        contacts = [r.position for r in records]

    return len(set(contacts)) * 1.0 / len(contacts)

@grouping(user_kwd=True, interaction=['screen', 'stop', 'physical'])
def percent_nocturnal(records, user):
    """Percentage of activity at night.

    By default, nights are 7pm-7am. Nightimes can be set in
    ``User.night_start`` and ``User.night_end``.
    """
    records = list(records)

    if len(records) == 0:
        return None

    if user.night_start < user.night_end:
        night_filter = lambda d: user.night_end > d.time() > user.night_start
    else:
        night_filter = lambda d: not(user.night_end < d.time() < user.night_start)

    return float(sum(1 for r in records if night_filter(r.datetime))) / len(records)

@grouping(interaction=['call', 'text', 'physical', 'screen', 'stop'])
def duration(records, direction=None):  # Consider removing direction argument
    """Duration of the user's sessions, grouped on subject of interaction.

    Parameters
    ----------
    direction : str, optional
        Filters the records by their direction: ``None`` for all records,
        ``'in'`` for incoming, and ``'out'`` for outgoing.
    """

    def _conversation_mean_duration(group):
        durations = [
            (conv[-1].datetime - conv[0].datetime).seconds
            for conv in _conversations_ext(group)
        ]
        return np.mean(durations)

    style = {
        'call': 'asis',
        'text': 'conversation',
        'physical': 'conversation',
        'screen': 'asis',
        'stop': 'asis'
    }
    
    records = list(records)

    if style[records[0].interaction] == 'conversation':
        interactions = defaultdict(list)
        for r in records:
            interactions[r.correspondent_id].append(r)

        durations = [
            _conversation_mean_duration(group) 
            for group in interactions.values()
        ]

    if style[records[0].interaction] == 'asis':
        durations = [r.duration for r in records]

    return np.mean(durations)

def _conversations(group, delta=datetime.timedelta(hours=1)):
    """Return iterator of grouped conversations.

    See :ref:`Using bandicoot <conversations-label>` for a definition of conversations.

    A conversation begins when one person sends a text-message to the other and ends when one of them makes a phone call
    or there is no activity between them for an hour.  
    """
    last_time = None
    results = []
    for g in group:

        if last_time is None or g.datetime - last_time < delta:
            if g.interaction == 'text':
                results.append(g)

            # A call always ends a conversation
            else:
                if len(results) != 0:
                    yield results
                    results = []

        else:
            if len(results) != 0:
                yield results

            if g.interaction == 'call':
                results = []
            else:
                results = [g]

        last_time = g.datetime

    if len(results) != 0:
        yield results

def _conversations_ext(group, delta=datetime.timedelta(hours=1)):
    """Return iterator of grouped conversations.

    See :ref:`Using bandicoot <conversations-label>` for a definition of conversations.

    A conversation begins when one person sends a text-message to the other and ends when one of them makes a phone call
    or there is no activity between them for an hour.  
    """
    last_time = None
    results = []
    for g in group:
        if last_time is None or g.datetime - last_time < delta:
            results.append(g)
            if g.interaction == "call" and g.duration > 1:
                yield results
                results = []

        else:
            if len(results) != 0:
                yield results
            results = [g]

        last_time = g.datetime

    if len(results) != 0:
        yield results

@grouping(interaction='callandtext')
def response_rate(records):
    """Response rate of the user (between 0 and 1).

    Considers text-conversations which began with an incoming text.  Response rate 
    is the fraction of such conversations in which the user sent a text (a response).  

    The following sequence of messages defines four conversations (``I`` for an
    incoming text, ``O`` for an outgoing text): ::

        I-O-I-O => Started with an incoming text and at least one outgoing text
        I-I-O-I => Started with an incoming text and at least one outgoing text
        I-I-I-I => Started with an incoming text but doesn't have outgoing texts
        O-O-I-O => Not starting with an incoming text

    Here, the ratio would be 2/3 as we have 3 conversations starting with an incoming text and 2 of them have at least one outgoing text.

    See :ref:`Using bandicoot <conversations-label>` for a definition of conversations.
    """
    records = list(records)

    interactions = defaultdict(list)
    for r in records:
        interactions[r.correspondent_id].append(r)

    def _response_rate(grouped):
        received, responded = 0, 0
        conversations = _conversations_ext(grouped)


        for conv in conversations:
            if len(conv) != 0:
                first = conv[0]
                if first.direction == 'in':
                    received += 1
                    if any((i.direction == 'out' for i in conv)):
                        responded += 1
        
        if received == 0:
            return None
        
        return responded * 1.0 / received

    # Group all records by their correspondent, and compute the response rate
    # for each
    rrates = filter(lambda v: v != None, map(_response_rate, interactions.values()))

    return summary_stats(rrates)

@grouping(interaction=['text', 'call'])
def response_delay(records):
    """Response delay of user in conversations grouped by interactions.

    Must also use call in order to seperate conversations.

    The following sequence of messages defines conversations (``I`` for an
    incoming text, ``O`` for an outgoing text, ``-`` for a one minute
    delay): ::

        I-O--I----O, we have a 60 seconds response delay and a 240 seconds response delay
        O--O---I--O, we have a 1200 seconds response delay
        I--II---I-I, we don't have a response delay. The user hasn't answered

    For this user, the distribution of response delays will be ``[60, 240, 60]``

    Notes
    -----
    See :ref:`Using bandicoot <conversations-label>` for a definition of conversations.
    Conversation are defined to be a series of text messages each sent no more than an hour 
    after the previous. The response delay can thus not be greater than one hour.
    """

    records = list(records)

    interactions = defaultdict(list)
    for r in records:
        interactions[r.correspondent_id].append(r)

    def _mean_response_delay(grouped):
        ts = ((b.datetime - a.datetime).total_seconds()
              for conv in _conversations_ext(grouped)
              for a, b in pairwise(conv)
              if b.direction == 'out' and a.direction == 'in')
        return np.mean(list(ts))

    delays = filter(
        lambda x: x > 0,
        [_mean_response_delay(group) for group in interactions.values()]
    )

    return summary_stats(delays)

@grouping(interaction=['text', 'call'])
def percent_initiated_conversations(records):
    """Percentage of conversations that have been initiated by the user.

    Each call and each text conversation is weighted as a single interaction.  

    See :ref:`Using bandicoot <conversations-label>` for a definition of conversations.
    """
    records = list(records)

    interactions = defaultdict(list)
    for r in records:
        interactions[r.correspondent_id].append(r)

    def _percent_initiated(grouped):
        mapped = [(1 if conv[0].direction == 'out' else 0, 1)
                  for conv in _conversations_ext(grouped)]
        return np.mean(mapped)

    all_couples = [_percent_initiated(i) for i in interactions.values()]

    return summary_stats(all_couples)

@grouping(interaction=['text', 'call'])
def percent_concluded_conversations(records):
    """Percentage of conversations that have been concluded by the user.

    Each text conversation is weighted as a single interaction.  

    #See :ref:`Using bandicoot <conversations-label>` for a definition of conversations.
    """
    records = list(records)

    interactions = defaultdict(list)
    for r in records:
        interactions[r.correspondent_id].append(r)

    def _percent_initiated(grouped):
        mapped = [(1 if conv[-1].direction == 'out' else 0, 1)
                  for conv in _conversations_ext(grouped)]
        return np.mean(mapped)

    all_couples = [_percent_initiated(i) for i in interactions.values()]

    return summary_stats(all_couples)

@grouping(interaction=['text', 'physical'])
def overlap_conversations_per_contacts(records):
    """Percent of conversation time that overlaps with other conversations.

    The following illustrates the concept of overlap. '-' denotes active conversation,
    '_' is idle time, and * underscores overlap. Each symbol is 1 minute.
        
        Conversation 1: ___-----______   # Overlap of two conversations. The total 
        Conversation 2: ______--------   # conversation time 11 minutes and overlap
        Conve. overlap:       **         # is 2 minutes: results in 2/11 overlap.

    #See :ref:`Using bandicoot <conversations-label>` for a definition of conversations.
    """
    records = list(records)

    interactions = defaultdict(list)
    for r in records:
        interactions[r.correspondent_id].append(r)

    def _timespans(grouped):
        to_ts = lambda dt: int(dt.strftime("%s"))
        ts = [(to_ts(conv[0].datetime), to_ts(conv[-1].datetime))
              for conv in _conversations_ext(grouped)]
        return ts

    timestamps = [
        ts 
        for i in interactions.values()
        for a,b in _timespans(i) 
        for ts in xrange(a,b)
    ]
    
    if len(timestamps) == 0:
        return None

    return (1 - len(set(timestamps)) / len(timestamps)) / len(interactions)

@grouping(interaction='screenandphysical')
def overlap_screen_physical(records):
    """Percent of screen time that overlaps with physical interaction time.

    The following illustrates the concept of overlap. '-' denotes active conversation,
    '_' is idle time, and * underscores overlap. Each symbol is 1 minute.
        
        Conversation 1: ___-----______   # Overlap of two conversations. The total 
        Conversation 2: ______--------   # conversation time 11 minutes and overlap
        Conve. overlap:       **         # is 2 minutes: results in 2/11 overlap.

    #See :ref:`Using bandicoot <conversations-label>` for a definition of conversations.
    """
    records = list(records)

    interactions = defaultdict(list)
    for r in records:
        if r.interaction == "physical":
            interactions[r.correspondent_id].append(r)

    def _timespans_physical(grouped):
        to_ts = lambda dt: int(dt.strftime("%s"))
        ts = [(to_ts(conv[0].datetime), to_ts(conv[-1].datetime))
              for conv in _conversations_ext(grouped, datetime.timedelta(hours=1.0/12))]
        return ts

    def _timespans_screen(r):
        to_ts = lambda dt: int(dt.strftime("%s"))
        return to_ts(r.datetime), to_ts(r.datetime) + r.duration

    timestamps_screen = [
        ts 
        for r in records if r.interaction == "screen"
        for ts in xrange(*_timespans_screen(r))
    ]

    timestamps_physical = [
        ts
        for i in interactions.values()
        for a, b in _timespans_physical(i)
        for ts in xrange(a, b)
    ]
    
    if len(timestamps_physical) == 0:
        return None

    timestamps_overlap = set(timestamps_screen) & set(timestamps_physical)

    return (1 - len(timestamps_overlap) / len(timestamps_physical)) / len(interactions)

@grouping(interaction='screen')
def active_days(records):
    """Number of days during which the user was active. 
    
    A user is considered active if he sends a text, receives a text, initiates 
    a call, receives a call, or has a mobility point.
    """

    days = set(r.datetime.date() for r in records)
    return len(days)

@grouping(interaction=['text', 'physical'])
def percent_ei_percent_interactions(records, percentage=0.8):
    """Percentage of contacts that account for 80%% of interactions."""

    records = list(records)
    if records == []:
        return None

    try:
        user_count = Counter(r.correspondent_id for r in records)
    except AttributeError:
        user_count = Counter(r.position for r in records)

    target = int(math.ceil(sum(user_count.values()) * percentage))
    user_sort = sorted(user_count.keys(), key=lambda x: user_count[x])

    while target > 0 and len(user_sort) > 0:
        user_id = user_sort.pop()
        target -= user_count[user_id]

    return (len(user_count) - len(user_sort)) / len(records)

@grouping(interaction=['call', 'stop'])
def percent_ei_percent_durations(records, percentage=0.8):
    """Percentage of contacts that account for 80%% of time spent.
    
    Optionally takes a percentage argument as a decimal (e.g., .8 for 80%%).  
    """

    records = list(records)
    if records == []:
        return None

    user_count = defaultdict(int)
    for r in records:
        if hasattr(r, "correspondent_id"):
            user_count[r.correspondent_id] += r.duration
        if hasattr(r, "position"):
            user_count[r.position] += r.duration
        else:
            user_count[r.datetime.strftime("%s")] += r.duration

    target = int(math.ceil(sum(user_count.values()) * percentage))
    user_sort = sorted(user_count.keys(), key=lambda x: user_count[x])

    while target > 0 and len(user_sort) > 0:
        user_id = user_sort.pop()
        target -= user_count[user_id]

    return (len(user_count) - len(user_sort)) / len(records)

@grouping(interaction=["call", "text"])
def balance_of_interactions(records, weighted=False, thresh=1):
    """Balance of out/(in+out) interactions.

    For every contact, the balance is the number of outgoing interactions 
    divided by the total number of interactions (in+out).

    .. math::

       \\forall \\,\\text{contact}\\,c,\\;\\text{balance}\,(c) = \\frac{\\bigl|\\text{outgoing}\,(c)\\bigr|}{\\bigl|\\text{outgoing}\,(c)\\bigr|+\\bigl|\\text{incoming}\,(c)\\bigr|}

    Parameters
    ----------
    weighted : str, optional
        If ``True``, the balance for each contact is weighted by
        the number of interactions the user had with this contact.
    """

    counter_out = 0
    counter = 0

    for r in records:
        if r.direction == 'out':
            counter_out += 1
        counter += 1

    return counter_out * 1.0 / counter


## ----------------- ##
## SPECIAL FUNCTIONS ##
## ----------------- ##

@grouping(interaction="physicalandstop")
def percent_interactions_campus(records):
    """Percent of interactions made on campus.

    NB: Any interaction type must be used in concatenation with stop
    """
    interactions_place = 0
    interactions = 0
    endtime = datetime.datetime.fromtimestamp(0)
    for r in records:
        if r.interaction == "stop":
            loc = r.event
            endtime = r.datetime + datetime.timedelta(0,r.duration)
        if r.interaction != "stop" and r.datetime < endtime:
            if loc == "campus":
                interactions_place += 1
            interactions += 1
            
    if interactions == 0:
        return None

    return interactions_place * 1.0 / interactions

@grouping(interaction="physicalandstop")
def percent_interactions_home(records):
    """Percent of interactions made at home.

    NB: Any interaction type must be used in concatenation with stop
    """
    interactions_place = 0
    interactions = 0
    endtime = datetime.datetime.fromtimestamp(0)
    for r in records:
        if r.interaction == "stop":
            loc = r.event
            endtime = r.datetime + datetime.timedelta(0,r.duration)
        if r.interaction != "stop" and r.datetime < endtime:
            if loc == "home":
                interactions_place += 1
            interactions += 1

    if interactions == 0:
        return None
    
    return interactions_place * 1.0 / interactions

@grouping(interaction="physicalandstop")
def percent_interactions_other(records):
    """Percent of interactions made at 'other'.

    NB: Any interaction type must be used in concatenation with stop
    """
    interactions_place = 0
    interactions = 0
    endtime = datetime.datetime.fromtimestamp(0)
    for r in records:
        if r.interaction == "stop":
            loc = r.event
            endtime = r.datetime + datetime.timedelta(0,r.duration)
        if r.interaction != "stop" and r.datetime < endtime:
            if loc == "other":
                interactions_place += 1
            interactions += 1

    if interactions == 0:
        return None
    
    return interactions_place * 1.0 / interactions

@grouping(interaction="physicalandstop")
def percent_outside_campus_from_campus(records):
    """Percent of interactions outside of campus that are from campus.

    NB: Any interaction type must be used in concatenation with stop
    """
    contacts_campus = set()
    contacts_other = set()
    endtime = datetime.datetime.fromtimestamp(0)
    for r in records:
        if r.interaction == "stop":
            loc = r.event
            endtime = r.datetime + datetime.timedelta(0,r.duration)
        if r.interaction != "stop" and r.datetime < endtime:
            if loc == "campus":
                contacts_campus.add(r.correspondent_id)
            if loc == "other":
                contacts_other.add(r.correspondent_id)

    if len(contacts_other) == 0:
        if len(contacts_campus) == 0:
            return None
        return 1.0

    return len(contacts_campus & contacts_other) * 1.0 / len(contacts_other)

@grouping(interaction="stop")
def percent_at_campus(records):
    """Commulative time spent at campus.

    NB: Only accepts stop.
    """
    counter_campus = 0
    counter = 0
    for r in records:
        if r.event == "campus":
            counter_campus += r.duration
        counter += r.duration
        
    if counter == 0:
        return None

    return counter_campus * 1.0 / counter

@grouping(interaction="stop")
def percent_at_home(records):
    """Commulative time spent at campus.

    NB: Only accepts stop.
    """
    counter_campus = 0
    counter = 0
    for r in records:
        if r.event == "home":
            counter_campus += r.duration
        counter += r.duration
        
    if counter == 0:
        return None

    return counter_campus * 1.0 / counter

@grouping(interaction="stop")
def percent_at_friday_bar(records):
    """Commulative time spent at campus.

    NB: Only accepts stop.
    """
    counter_campus = 0
    counter = 0
    for r in records:
        if r.event == "friday_bar":
            counter_campus += r.duration
        counter += r.duration
        
    if counter == 0:
        return None

    return counter_campus * 1.0 / counter

@grouping(interaction=['physical', 'stop'])
def percent_contacts_less(records, cutoff=1):
    """Percent of users contacts that has only been observed in 'cutoff' or less conversations.

    NB: Only accepts stop.
    """
    records = list(records)
    interactions = defaultdict(list)

    if hasattr(records[0], 'correspondent_id'):

        for r in records:
            interactions[r.correspondent_id].append(r)

        interaction_counts = [
            len(list(_conversations_ext(group))) for group in interactions.values()
        ]

    else:

        for r in records:
            interactions[r.position].append(r)

        interaction_counts = [
            len(group) for group in interactions.values()
        ]
    
    if interaction_counts == 0:
        return None
    
    return sum([1 for c in interaction_counts if c <= cutoff]) * 1.0 / len(interaction_counts)

@grouping(interaction="screenandtext")
def first_seen_response_rate(records):
    """Rate of first seen responses to texts
    """
    def _first_session_id():
        return session_id if r.datetime < endtime else session_id + 1

    responses = []

    pending = {}
    endtime = datetime.datetime.fromtimestamp(0)

    session_id = -1

    for r in records:
        if r.interaction == "screen":
            session_id += 1
            endtime = r.datetime + datetime.timedelta(0,r.duration)
        if r.interaction == "text" and r.direction == "in":
            if r.correspondent_id not in pending:
                pending[r.correspondent_id] = _first_session_id()
        if r.interaction == "text" and r.direction == "out":
            if r.correspondent_id in pending:
                if pending[r.correspondent_id] == session_id:
                    responses.append(1)
                elif pending[r.correspondent_id] < session_id:
                    responses.append(0)
                else:
                    # Interpreter only reaches here if there exists inconsistency
                    # such as text out not contained in session. Such events are 
                    # disregarded
                    pass
                del pending[r.correspondent_id]

    return np.mean(responses)










