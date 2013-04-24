import httplib
import urlparse
import json
from bs4 import BeautifulSoup

class Period(object):
    def __init__(self, year='', month='', day=''):
        if year:
            self.y = int(year)
        if month:
            self.m = int(month)
        if day:
            self.d = int(day)

class Position(object):
    def __init__(self, title='', org='', period=None):
        self.title = title
        if org:
            self.org = org
        if period and period[0]:
            period = map(lambda p:p.replace('/','-'), period)
            self.period = map(lambda p:Period(*(p.split('-')[:3])).__dict__, period)
            end = self.period[1]
            start = self.period[0]
            print start, end
            if end.has_key('y'):
		d_year = (end['y'] - start['y']) * 365
		d_month = max(end.get('m',1) - start.get('m',1), 0) * 30
		d_day = max(end.get('d',1) - start.get('d',1), 0)
		self.duration = d_year+d_month+d_day
    def __repr__(self):
        if hasattr('org'):
            return (u'%s @ %s' % (self.title, self.org)).encode('utf-8')
        else:
            return (u'%s' % (self.title,)).encode('utf-8')

class EducationSummary(object):
    def __init__(self, instituion='', degree='', period=None):
        self.inst = instituion
        self.deg = degree
        if period and period[0]:
            period = map(lambda p:p.replace('/','-'), period)
            self.period = map(lambda p:Period(*(p.split('-')[:3])).__dict__, period)

class CandidatePage(object):
    @classmethod
    def from_url(cls, url, conn=None):
        pr = urlparse.urlparse(url)
        server = pr.netloc
        uri = pr.path
        conn = httplib.HTTPConnection(server)
        conn.request('GET', uri)
        res = conn.getresponse()
        if res.status == httplib.OK:
            cp = CandidatePage.from_html(res.read())
            cp.url = url
            cp.url.replace('http://','')
            return cp
        elif res.status == httplib.MOVED_PERMANENTLY:
            newurl = res.getheader('location')
            return CandidatePage.from_url(newurl, conn)

    @classmethod
    def from_html(cls, html_page):
        cp = cls()
        cp.parse(html_page)
        return cp

    def parse(self, html_page):
        soup = BeautifulSoup(html_page)
        self.last_nm = get_last_name(soup)
        self.first_nm = get_first_name(soup)
        self.location = get_location(soup)
        self.curr_job = get_current_job(soup)
        self.past_job = get_past_job(soup)
        skills = get_skills(soup)
        if skills:
            self.skills = skills
        education = get_education_details(soup)
        if education:
            self.edu = education
        positions = get_positions(soup)
        if positions:
            self.positions = positions
        self.cxs = get_connection(soup)

    def to_json(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=4)

    def to_dict(self):
        return self.__dict__

def get_positions(soup):
    ret = []
    for pos, period in zip(soup.findAll(**{'class':'postitle'}),
                           soup.findAll(**{'class':'period'})):
        title = pos.find(**{'class':'title'})
        title = title.string if title else ''
        org = pos.find(**{'class':'org summary'})
        org = org.string if org else ''

        dtstart = period.find(**{'class':'dtstart'})
        dtend = period.find(**{'class':'dtend'})
        dtnow = period.find(**{'class':'dtstamp'})
        dte = dtend or dtnow

        dtstart_str = dtstart.get('title') if dtstart else ''
        dtend_str = dte.get('title') if dte else ''

        period_tuple = (dtstart_str, dtend_str)

        ret.append(Position(title, org, period_tuple).__dict__)
    return ret


def get_last_name(soup):
    fullname = soup.find(**{'class':'full-name'})
    return fullname.find(**{'class':'family-name'}).string.strip().capitalize()

def get_first_name(soup):
    fullname = soup.find(**{'class':'full-name'})
    return fullname.find(**{'class':'given-name'}).string.strip().capitalize()

def get_location(soup):
    location = soup.find(**{'class':'locality'})
    if not location:
        return ''
    return location.string.strip()

def get_connection(soup):
    connection = soup.find(**{'class':'overview-connections'})
    if not connection:
        return 0
    num = connection.find(name='strong')
    if num:
        return int(num.string.replace('+',''))
    else:
        return 0

def get_job(soup, clstag):
    argd = {'class':clstag}
    current_job = None
    for element in soup.find_all(**argd):
        if element.name == 'dd':
            current_job = element

    if not current_job:
        return None
    # the first list current job element
    jb = current_job.find('li')
    if not jb:
        return None
    strings = list(jb.strings)
    if not strings:
        return None

    # convert the result to 'Position' object
    jp = Position(strings[0].strip().capitalize())
    if len(strings) > 3:
        jp.org = strings[3].strip()
    return jp.__dict__

def get_current_job(soup):
    return get_job(soup, 'summary-current')

def get_past_job(soup):
    return get_job(soup, 'summary-past')


def get_education_details(soup):
    edu_details = soup.find(**{'id':'profile-education'})
    if not edu_details:
        return []
    edu_orgs = edu_details.findAll(**{'class':'summary fn org'})
    edu_history = [e.string.strip() for e in edu_orgs]
    edu_degrees = [d.string.strip() for d in
            edu_details.findAll(**{'class':'degree'})]
    edu_periods = edu_details.findAll(**{'class':'period'})

    dtstarts = [p.find(**{'class':'dtstart'}) for p in edu_periods]
    dtends = [p.find(**{'class':'dtend'}) for p in edu_periods]
    periods = [(s.get('title'), e.get('title')) for s, e in zip(dtstarts,
        dtends) if s and e]

    edu_summary = [EducationSummary(inst, deg, p).__dict__
            for inst, deg, p in zip(edu_history, edu_degrees, periods)]
    return edu_summary

def get_skills(soup):
    skills = soup.find(**{'class':'skills'})
    if not skills:
        return []
    return [f.strip() for f in skills.strings if f.strip()]

if __name__ == '__main__':
    urls = [
      # url ...
      ]

    for url in urls: 
        cp = CandidatePage.from_url(url)
        print cp.to_json()

