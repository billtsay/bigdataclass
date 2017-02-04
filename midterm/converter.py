'''
Created on Apr 12, 2016

@author: cloudera
'''
#from python.streaming.pig_util import outputSchema
#import python.streaming.pig_util.outputSchema as outputSchema

'''
{
    "business_id": "5UmKMjUEUNdYWqANhGckJw",
    "full_address": "4734 Lebanon Church Rd\\nDravosburg, PA 15034",
    "hours": 
    {
        "Friday": 
        {
            "close": "21:00",
            "open": "11:00"
        },

        "Tuesday": 
        {
            "close": "21:00",
            "open": "11:00"
        },

        "Thursday": 
        {
            "close": "21:00",
            "open": "11:00"
        },

        "Wednesday": 
        {
            "close": "21:00",
            "open": "11:00"
        },

        "Monday": 
        {
            "close": "21:00",
            "open": "11:00"
        }
    },

    "open": true,
    "categories": 
    [
        "Fast Food",
        "Restaurants"
    ],

    "city": "Dravosburg",
    "review_count": 4,
    "name": "Mr Hoagie",
    "neighborhoods": 
    [
        
    ],

    "longitude": -79.9007057,
    "state": "PA",
    "stars": 4.5,
    "latitude": 40.3543266,
    "attributes": 
    {
        "Take-out": true,
        "Drive-Thru": false,
        "Good For": 
        {
            "dessert": false,
            "latenight": false,
            "lunch": false,
            "dinner": false,
            "brunch": false,
            "breakfast": false
        },

        "Caters": false,
        "Noise Level": "average",
        "Takes Reservations": false,
        "Delivery": false,
        "Ambience": 
        {
            "romantic": false,
            "intimate": false,
            "classy": false,
            "hipster": false,
            "divey": false,
            "touristy": false,
            "trendy": false,
            "upscale": false,
            "casual": false
        },

        "Parking": 
        {
            "garage": false,
            "street": false,
            "validated": false,
            "lot": false,
            "valet": false
        },

        "Has TV": false,
        "Outdoor Seating": false,
        "Attire": "casual",
        "Alcohol": "none",
        "Waiter Service": false,
        "Accepts Credit Cards": true,
        "Good for Kids": true,
        "Good For Groups": true,
        "Price Range": 1
    },

    "type": "business"
}

'''


from com.xhaus.jyson import JysonCodec as json

@outputSchema("categories:bag{t:(business_id:chararray,category:chararray)}")
def categories(line):
    b_json = json.loads(line)
    cats = []
    for x in b_json["categories"]:
        cats.append((b_json["business_id"], x))
        
    return cats
    
def business(line):
    b_json = json.loads(line)
    biz = [b_json["business_id"], b_json["name"], b_json["city"], b_json["type"], str(b_json["review_count"]), str(b_json["stars"]),  str(b_json["longitude"]), str(b_json["latitude"]) ]
    
    return ",".join(biz).encode('utf-8')


def revwbymonth(line):
    r_json = json.loads(line)
    # date in yyyy-mm-dd format
    month = r_json["date"].split("-")[1]
    
    revs = [r_json["business_id"], r_json["user_id"], str(r_json["stars"]), month ]
    
    return ",".join(revs).encode('utf-8')


def reviewers(line):
    r_json = json.loads(line)
    revs = [r_json["business_id"], r_json["user_id"], str(r_json["stars"]) ]
    
    return ",".join(revs).encode('utf-8')


def users(line):
    u_json = json.loads(line)
    users = [u_json["user_id"], u_json["name"], str(u_json["average_stars"]), str(u_json["review_count"]) ]
    
    return ",".join(users).encode('utf-8')


line = '''
{"business_id": "5UmKMjUEUNdYWqANhGckJw", "full_address": "4734 Lebanon Church Rd\\nDravosburg, PA 15034", "hours": {"Friday": {"close": "21:00", "open": "11:00"}, "Tuesday": {"close": "21:00", "open": "11:00"}, "Thursday": {"close": "21:00", "open": "11:00"}, "Wednesday": {"close": "21:00", "open": "11:00"}, "Monday": {"close": "21:00", "open": "11:00"}}, "open": true, "categories": ["Fast Food", "Restaurants"], "city": "Dravosburg", "review_count": 4, "name": "Mr Hoagie", "neighborhoods": [], "longitude": -79.9007057, "state": "PA", "stars": 4.5, "latitude": 40.3543266, "attributes": {"Take-out": true, "Drive-Thru": false, "Good For": {"dessert": false, "latenight": false, "lunch": false, "dinner": false, "brunch": false, "breakfast": false}, "Caters": false, "Noise Level": "average", "Takes Reservations": false, "Delivery": false, "Ambience": {"romantic": false, "intimate": false, "classy": false, "hipster": false, "divey": false, "touristy": false, "trendy": false, "upscale": false, "casual": false}, "Parking": {"garage": false, "street": false, "validated": false, "lot": false, "valet": false}, "Has TV": false, "Outdoor Seating": false, "Attire": "casual", "Alcohol": "none", "Waiter Service": false, "Accepts Credit Cards": true, "Good for Kids": true, "Good For Groups": true, "Price Range": 1}, "type": "business"}
'''

#print categories(line)
print business(line)

