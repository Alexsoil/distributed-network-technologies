from bs4 import BeautifulSoup    #dependancies
import requests                  #dependancies
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

#function that parses the scientists CSV and opens all the links fetching info from their bio tables about Awards and Alma Mater. Then saves it in a JSON file.
def UpdateFinalRecord():

    awards_Record = []
    alma_mater_Record = []
    list_of_dictionaries = []
    has_no_table = []
    has_no_awards = []
    has_no_alma = []

    with open(smallTempCSV, 'r') as names_links:
        scientists_links = csv.reader(names_links, delimiter= ',')
        line_count = 0

        #For every scientist in the csv, visit the link of their page and fetch the awards and alma mater. Each time it creates a new soup tree.
        for current_scientist in scientists_links:
            if line_count == 0:
                line_count =+ 1
                continue

            #delete the contents of the lists for the new iteration
            awards_Record.clear()
            alma_mater_Record.clear()

            curr_url = current_scientist[1]     #current scientist's url
            curr_name = current_scientist[0]    #current scientist's name

            result = requests.get(curr_url)
            doc = BeautifulSoup(result.text, "html.parser")

            if doc.find(class_ = 'infobox'):
                
                #specify the desired position in the soup tree, that of the info table.
                pos = doc.find(class_ = 'infobox').tbody

                #handle unexpected errors as exceptions
                try:
                    if pos.find('th', string= re.compile('Alma*')):
                        alma_mater = pos.find('th', string= re.compile('Alma*')).next_sibling.find_all('a')
                        #scope in the Alma mater section of the table and find all the listed universities. Afterwards, iterate through them and capture all titles.
                        for i in alma_mater:
                            try:        #if there's a link with no title, throw no errors
                                alma_mater_Record.append(i["title"])
                            except:
                                pass
                    elif pos.find('th', string= re.compile('Education*')):
                        alma_mater = pos.find('th', string= re.compile('Education*')).next_sibling.find_all('a')
                        #scope in the Alma mater section of the table and find all the listed universities. Afterwards, iterate through them and capture all titles.
                        for i in alma_mater:
                            try:        #if there's a link with no title, throw no errors
                                alma_mater_Record.append(i["title"])
                            except:
                                pass
                    else:
                        has_no_alma.append([curr_name, curr_url])
                        print("\t\t" + curr_name + ' has no alma mater record')


                    if pos.find('th', text= 'Awards'):
                        #scope in the awards section of the table and find all the listed awards. Afterwards, iterate through them and capture all titles.
                        awards = pos.find('th', text= 'Awards').next_sibling.find_all('a')
                        for i in awards:
                            try:
                                awards_Record.append(i['title'])
                            except:
                                pass
                    else:
                        has_no_awards.append([curr_name, curr_url])
                        print("\t\t" + curr_name + ' has no award record')
                    
                    #success check for the current scientist
                    print("All good with " + curr_name)

                except:
                    print("\t\tWith " + curr_name + " occured an error!!")

            else:
                has_no_table.append([curr_name, curr_url])
                print(curr_name + ' has no info table')


            #Create a temporal dictionary to add it to the list for saving purposes.
            temp_dict = {
                "scientist": curr_name,
                "Alma mater": alma_mater_Record,
                "Awards": awards_Record
            }
            #Add the fetched info to the list as a dictionary for easier cenverting to json object.
            list_of_dictionaries.append(temp_dict)


    print(list_of_dictionaries[:10])
    
    with open("No_Awards.csv", 'w') as noAwards:
        writer = csv.writer(noAwards)
        writer.writerow(['Scientist', 'Link'])
        writer.writerows(has_no_awards)

    with open("No_Alma_Mater.csv", 'w') as noAlma:
        writer = csv.writer(noAlma)
        writer.writerow(['Scientist', 'Link'])
        writer.writerows(has_no_alma)
    
    with open("Final_Record.json", 'w') as outfile:
        json.dump(list_of_dictionaries, outfile)



def test():
    awards_Record = []
    alma_mater_Record = []
    
    result = requests.get("https://en.wikipedia.org/wiki/Lenore_Blum")
    doc = BeautifulSoup(result.text, "html.parser")

    #specify the desired position in the soup tree, that of the info table.
    if doc.find(class_ = 'infobox'):
        pos = doc.find(class_ = 'infobox').tbody
    #test = pos.find('th', string = re.compile('Awards')).next_sibling.stripped_strings

        try:
            if pos.find('th', string= re.compile('Alma*')):
                #scope in the Alma mater section of the table and find all the listed universities. Afterwards, iterate through them and capture all titles.
                alma_mater = pos.find('th', string= re.compile('Alma*')).next_sibling.find_all('a')
                for i in alma_mater:
                    try:
                        alma_mater_Record.append(i["title"])
                    except:
                        pass
            else:
                print("David P. Anderson" + " - alma -")


            #scope in the awards section of the table and find all the listed awards. Afterwards, iterate through them and capture all titles.
            awards = pos.find('th', string = re.compile('Awards')).next_sibling.find_all('a')
            for i in awards:
                try:
                    awards_Record.append(i['title'])
                except:
                    pass
        except:
            print("David P. Anderson" + ' - awards')
    
    else:
        print("Has not table")

    print(awards_Record, '\n', alma_mater_Record)

def prettify_json():
    with open('Final_Record.json')
#Gets exceptions for no apparent reason - Must check!
UpdateFinalRecord()
#test()
