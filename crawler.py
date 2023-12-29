from bs4 import BeautifulSoup
import requests
import csv
import re
import json

#CSV, JSON files declaration:
smallTempCSV = "Scientist&Links.csv"
Final_Record = "FinalRecord.json"


##############################################################################################################


#function that pasres the initial link of Wikipedia and saves all the scientists names and the links to their pages into a csv for later use.
def Temporal_csv_Record():

    temp_table_of_scientists = []
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    #Making the soup.
    url = "https://en.wikipedia.org/wiki/List_of_computer_scientists"
    result = requests.get(url)
    doc = BeautifulSoup(result.text, "html.parser")
    
    #Iterates through all letters in the alphabet and uses them to pinpoint each h2 tag in the least for all the names.
    #If a letter is not found in the list, raises an exception and it continues with next letter (Happens with X and Q).
    for letter in letters:

        try:
            tags =  doc.find(class_ = "mw-content-ltr mw-parser-output").find(class_ = "mw-headline", id = letter) 

            current_ul = tags.parent.next_sibling.next_sibling.find_all("li")

    #Append the temporal list of scientists with the links to their wikipedia sites.
            for li in current_ul:
                temp_table_of_scientists.append([li.a["title"], "https://en.wikipedia.org" + li.a["href"]])
            
        except:
            pass

    #create the csv for the scientists and the links.
    with open(smallTempCSV, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Scientist', 'Link'])
        csvwriter.writerows(temp_table_of_scientists)


def UpdateFinalRecord():

    awards_Record = []
    alma_mater_Record = []
    list_of_dictionaries = []

    with open(smallTempCSV, 'r') as names_links:
        scientists_links = csv.reader(names_links, delimiter= ',')
        line_count = 0

        #For every scientist in the csv, visit the link of their page and fetch the awards and alma mater. Each time it creates a new soup tree.
        for current_scientist in scientists_links:
            if line_count == 0:
                line_count =+ 1
                continue
            
            curr_url = current_scientist[1]     #current scientist's url
            curr_name = current_scientist[0]    #current scientist's name

            result = requests.get(curr_url)
            doc = BeautifulSoup(result.text, "html.parser")

            #specify the desired position in the soup tree, that of the info table.
            pos = doc.find(class_ = 'infobox biography vcard').tbody


            try:
                #scope in the Alma mater section of the table and find all the listed universities. Afterwards, iterate through them and capture all titles.
                alma_mater = pos.find('th', text= re.compile('Alma*')).next_sibling.ul.find_all('li')
                for i in alma_mater:
                    alma_mater_Record.append(i.a["title"])
            except:
                print(curr_name + " - alma -")
                pass


            try:
                #scope in the awards section of the table and find all the listed awards. Afterwards, iterate through them and capture all titles.
                awards = pos.find('th', text= 'Awards').next_sibling.ul.find_all('li')
                for i in awards:
                    awards_Record.append(i.a['title'])
            except:
                print(curr_name + ' - awards')
                pass

            #Create a temporal dictionary to add it to the list for saving purposes.
            temp_dict = {
                "scientist": curr_name,
                "Alma mater": alma_mater_Record,
                "Awards": awards_Record
            }
            #Add the fetched info to the list as a dictionary for easier cenverting to json object.
            list_of_dictionaries.append(temp_dict)

            #delete the contents of the lists for the new iteration
            awards_Record = []
            alma_mater_Record = []

    print(list_of_dictionaries)
    with open("Final_Record", 'w') as outfile:
        json.dumps(list_of_dictionaries, outfile)



def test():
    awards_Record = []
    alma_mater_Record = []
    
    result = requests.get("https://en.wikipedia.org/wiki/Manindra_Agrawal")
    doc = BeautifulSoup(result.text, "html.parser")

    #specify the desired position in the soup tree, that of the info table.
    pos = doc.find(class_ = 'infobox biography vcard').tbody
    test = pos.find('th', string = re.compile('Awards')).next_sibling.stripped_strings



    try:
        #scope in the Alma mater section of the table and find all the listed universities. Afterwards, iterate through them and capture all titles.
        alma_mater = pos.find('th', string= re.compile('Alma*')).next_sibling.stripped_strings#.ul.find_all('li')
        for i in alma_mater:
            alma_mater_Record.append(i)#.a["title"])
    except:
        print("David P. Anderson" + " - alma -")
        pass


    try:
        #scope in the awards section of the table and find all the listed awards. Afterwards, iterate through them and capture all titles.
        awards = pos.find('th', string = re.compile('Awards')).next_sibling.stripped_strings
        for i in awards:
            awards_Record.append(i)
    except:
        print("David P. Anderson" + ' - awards')
        pass
    
    print(awards_Record, '\n' , alma_mater_Record)
#Gets exceptions for no apparent reason - Must check!
test()