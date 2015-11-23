from xml.dom import minidom

xmldoc = minidom.parse('ratings.xml')
itemlist = xmldoc.getElementsByTagName('entry')

#Number of categories
cat_num = 51


def get_keywords(str):
    keywordlst = []
    kw = str.split(",")

    for k in kw:
        ks = k.strip()
        keywordlst.append(ks)

    return keywordlst


def get_category(itemlistInd):
    cat = itemlist[itemlistInd].getElementsByTagName('category')[0].firstChild.nodeValue
    abbrev = itemlist[itemlistInd].getElementsByTagName('abbrev')[0].firstChild.nodeValue

    ratings = itemlist[itemlistInd].getElementsByTagName('ratings')[0]
    na = ratings.getElementsByTagName('Na')[0].firstChild.nodeValue
    ma = ratings.getElementsByTagName('Ma')[0].firstChild.nodeValue
    ps = ratings.getElementsByTagName('Ps')[0].firstChild.nodeValue
    op = ratings.getElementsByTagName('Op')[0].firstChild.nodeValue
    co = ratings.getElementsByTagName('Co')[0].firstChild.nodeValue
    ex = ratings.getElementsByTagName('Ex')[0].firstChild.nodeValue
    ag = ratings.getElementsByTagName('Ag')[0].firstChild.nodeValue
    ne = ratings.getElementsByTagName('Ne')[0].firstChild.nodeValue

    keywords = itemlist[itemlistInd].getElementsByTagName('keywords')[0].firstChild.nodeValue

    return (abbrev, ("na", na), ("ma", ma), ("ps", ps), ("op", op), ("co", co), ("ex", ex), ("ag", ag), ("ne", ne), get_keywords(keywords))


def get_categories():    
    list = []
    for i in range(cat_num):
        list.append(get_category(i))
    return list
