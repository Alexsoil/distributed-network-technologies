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
def Update_Scientists_Record():

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
            print("There's no scientist in the letter: " + letter)

    #create the csv for the scientists and the links.
    with open(smallTempCSV, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Scientist', 'Link'])
        csvwriter.writerows(temp_table_of_scientists)
    
    print("Update completed!")


#function that parses the scientists CSV and opens all the links fetching info from their bio tables about Awards and Alma Mater. Then saves it in a JSON file.
def Update_Final_Record():

    awards_Record = []    #lit of the awards a scientist has
    alma_mater_Record = []    #list of institutions a scientist has graduated from
    list_of_dictionaries = []    #list that will hold the dictionaries needed for the json file
    has_no_table = []    #list that holds the names and links of scientists with no info table in wikipedia
    has_no_awards = []    #list that holds the names and links of scientists with no awards  in wikipedia
    has_no_alma = []    ##list that holds the names and links of scientists with no Alma mater in wikipedia

    with open(smallTempCSV, 'r') as names_links:    #Read from the csv of the scientists
        scientists_links = csv.reader(names_links, delimiter= ',')
        line_count = 0    #utility usage

        #For every scientist in the csv, visit the link of their page and fetch the awards and alma mater. Each time it creates a new soup tree.
        for current_scientist in scientists_links:
            if line_count == 0:    #skip the first iteration, it'll read the name and link row of the csv, useless
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
                        print("\t\t" + curr_name + ' has no alma mater record (skill issue :C )')


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
                        print("\t\t" + curr_name + ' has no award record (cry about it :C )')
                    
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
    
    with open("No_Awards.csv", 'w') as noAwards:
        writer = csv.writer(noAwards)
        writer.writerow(['Scientist', 'Link'])
        writer.writerows(has_no_awards)

    with open("No_Alma_Mater.csv", 'w') as noAlma:
        writer = csv.writer(noAlma)
        writer.writerow(['Scientist', 'Link'])
        writer.writerows(has_no_alma)
    
    with open("Final_Record.json", 'w') as outfile:
        json.dump(list_of_dictionaries, outfile, indent= 4)

#function designed for testing purposes, feel free to ignore!
def test():
    awards_Record = []
    alma_mater_Record = []
    
    result = requests.get("https://en.wikipedia.org/wiki/Konrad_Zuse")
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

print("1. Update the list of scientists.\n2. Update info of scientists.\n3. Quit.\n")
option = str(input("\n"))

if option == "1":
    Update_Scientists_Record()
elif option == "2":
    Update_Final_Record()
else:
    quit()
