import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager, Link
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html, etree
from lxml.html import html5parser
from lxml.html.clean import Cleaner
from collections import Counter
import re, os
from time import time

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set()
             if not os.path.exists("successful_urls.txt") else
             set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

# Analytics logging variables
subdomain_frequencies = dict()
invalid_count = 0
max_outlinks = 0
max_outlink_url = ""


@Producer(ProducedLink, Link)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):
    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "49787805_67913318_88551199"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 UnderGrad 49787805 67913318 88551199 TEST"

        self.frame = frame
        assert (self.UserAgentString != None)
        assert (self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get_new(OneUnProcessedGroup):
            print "Got a Group"
            global invalid_count
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    update_subdomain_frequencies(l)
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
                else:
                    invalid_count += 1 # keeps track of invalid link count
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        with open("analytics.txt", "w") as analytics:
            analytics.write(("List of subdomains visited and frequency of visits:\n").encode("utf-8"))
            for key, value in d.iteritems():
                analytics.write(key + ": " + str(value) + "\n")
            analytics.write(("\nNumber of invalid links: " + invalid_count + "\n").encode("utf-8"))
            analytics.write(("\nPage with most out links: " + max_outlink_url + " with "\
                             + str(max_outlinks) + ".\n").encode("utf-8"))
        pass


def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))


def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas


#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''


def extract_next_links(rawDatas):
    global max_outlinks
    global max_outlink_url
    outputLinks = list()
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    The frontier takes care of that.

    Suggested library: lxml
    '''

    # cleaner = Cleaner(page_structure = False, links = False) # clean(remove) scripts, special tags, css style annotations, etc
    for raw_content_obj in rawDatas:

        if should_extract_urls(raw_content_obj):
            try:
                
                content = raw_content_obj.content
                # content = cleaner.clean_html(content)

                e = html5parser.fromstring(content)  # Parse html5 content into element
                doc = html.fromstring(html.tostring(e)) # Weird workaround when using html5parser.from_string and html.fromstring
                                                        # because they return different objects
                doc.make_links_absolute(raw_content_obj.url, resolve_base_href=True)

                link_count = 0
                for e, a, l, p in doc.iterlinks():  # Get (element, attribute, link, pos) for every link in doc
                    outputLinks.append(l)
                    link_count += 1
                    #print l

                if (link_count > max_outlinks):
                    max_outlinks = link_count
                    max_outlink_url = raw_content_obj.url
                
            except etree.XMLSyntaxError as e:
                print "Error on url " + raw_content_obj.url + " " + str(e)
                raw_content_obj.bad_url = True
    
    return outputLinks


def should_extract_urls(raw_content_obj):
    # print "Content code: "  + raw_content_obj.http_code
    return raw_content_obj.http_code == "200"  # HTTP Response Code 200 OK


def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    
    print (url)

    if parsed.scheme not in set(["http", "https"]):
        return False

    path = parsed.path  # Get url path
    #paths_split = [s for s in path.split("/") if s != ""]  # Split path individually, remove empty space
    paths_split = path.split("/")

    # print paths_split

    word, freq = Counter(paths_split).most_common(1)[0]
    if freq > 3:  # Check if there is a path that is duplicated > 3 times 
        print "too many duplicates: ", url
        return False

    #for string in dynamic_strings:
    #    if string in url:
    #        return False

    if "?" in url and "=" in url:
        print "? and = in url: ", url
        return False

    if "ugrad/index.php/" in url and len(parsed.path) > 13:
        print (url, "ugrad rekt")
        return False
    
    if "ugrad/index/" in url and len(parsed.path) > 9:
        print (url, "ugrad rekt")
        return False
   
    
    
    try:
        return ".ics.uci.edu" in parsed.hostname \
               and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                + "|thmx|mso|arff|rtf|jar|csv" \
                                + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)



def update_subdomain_frequencies(url):
    global subdomain_frequencies
    parsed = urlparse(url)
    hostname = parsed.hostname
    if (hostname[:3] != "www"):
        subdomain = parsed.hostname[:-12]
        if (subdomain in subdomain_frequencies):
            subdomain_frequencies[subdomain] += 1
        else:
            subdomain_frequencies[subdomain] = 1
