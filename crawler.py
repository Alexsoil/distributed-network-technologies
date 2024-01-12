from bs4 import BeautifulSoup  # dependancies
import requests  # dependancies
import csv
import re
import json

# CSV, JSON files declaration:
smallTempCSV = "urls.csv"
Final_Record = "scientists.json"


##############################################################################################################


# function that pasres the initial link of Wikipedia and saves all the scientists names and the links to their pages into a csv for later use.
def Update_Scientists_Record():
    temp_table_of_scientists = []
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    # Making the soup.
    url = "https://en.wikipedia.org/wiki/List_of_computer_scientists"
    result = requests.get(url)
    doc = BeautifulSoup(result.text, "html.parser")

    # Iterates through all letters in the alphabet and uses them to pinpoint each h2 tag in the least for all the names.
    # If a letter is not found in the list, raises an exception and it continues with next letter (Happens with X and Q).
    for letter in letters:
        try:
            tags = doc.find(class_="mw-content-ltr mw-parser-output").find(
                class_="mw-headline", id=letter
            )
            current_ul = tags.parent.next_sibling.next_sibling.find_all("li")

            # Append the temporal list of scientists with the links to their wikipedia sites.
            for li in current_ul:
                temp_table_of_scientists.append(
                    [li.a["title"], "https://en.wikipedia.org" + li.a["href"]]
                )

        except:
            print("There's no scientist in the letter: " + letter)

    # create the csv for the scientists and the links.
    with open(smallTempCSV, "w", encoding="utf-8", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["Scientist", "Link"])
        csvwriter.writerows(temp_table_of_scientists)

    print("Update completed!")


# function that parses the scientists CSV and opens all the links fetching info from their bio tables about Awards and Alma Mater. Then saves it in a JSON file.
def Update_Final_Record():
    list_of_dictionaries = (
        []
    )  # list that will hold the dictionaries needed for the json file
    has_no_table = (
        []
    )  # list that holds the names and links of scientists with no info table in wikipedia
    has_no_awards = (
        []
    )  # list that holds the names and links of scientists with no awards  in wikipedia
    has_no_alma = (
        []
    )  ##list that holds the names and links of scientists with no Alma mater in wikipedia

    alma_patterns = ["Alma*", "Education*"]
    alma_patterns = [re.compile(p) for p in alma_patterns]

    with open(
        smallTempCSV, "r", encoding="utf-8", newline=""
    ) as names_links:  # Read from the csv of the scientists
        scientists_links = csv.reader(names_links, delimiter=",")
        next(scientists_links, None)  # Skip header
        # For every scientist in the csv, visit the link of their page and fetch the awards and alma mater.
        # Each time it creates a new soup tree.
        for name, url in scientists_links:
            awards = []
            alma_mater = []

            result = requests.get(url)
            doc = BeautifulSoup(result.text, "html.parser")

            if doc.find(class_="infobox"):
                # specify the desired position in the soup tree, that of the info table.
                pos = doc.find(class_="infobox").tbody

                # handle unexpected errors as exceptions
                try:
                    # Alma mater
                    for pattern in alma_patterns:
                        alma_section = pos.find("th", string=pattern)
                        if alma_section is None:
                            continue

                        tags = alma_section.next_sibling.find_all("a")
                        for tag in tags:
                            # Filter out Wikipedia tags, like [citation needed]
                            if not any(parent.name == "sup" for parent in tag.parents):
                                alma_mater.append(tag.string)
                        break
                    else:
                        has_no_alma.append([name, url])
                        print(name + " has no alma mater record (skill issue :C )")

                    # Awards
                    awards_section = pos.find("th", string="Awards")
                    if awards_section is not None:
                        tags = awards_section.next_sibling.find_all("a")
                        for tag in tags:
                            if not any(parent.name == "sup" for parent in tag.parents):
                                awards.append(tag.string)
                    else:
                        has_no_awards.append([name, url])
                        print(name + " has no award record (cry about it :C )")

                    # success check for the current scientist
                    print("Done with " + name)

                except:
                    print("With " + name + " occured an error!!")

            else:
                has_no_table.append([name, url])
                print(name + " has no info table")

            # Create a temporal dictionary to add it to the list for saving purposes.
            temp_dict = {
                "name": name,
                "alma_mater": alma_mater,
                "awards": awards,
            }
            # Add the fetched info to the list as a dictionary for easier cenverting to json object.
            list_of_dictionaries.append(temp_dict)

    with open("no_awards.csv", "w", encoding="utf-8", newline="") as noAwards:
        writer = csv.writer(noAwards)
        writer.writerow(["Scientist", "Link"])
        writer.writerows(has_no_awards)

    with open("no_alma_mater.csv", "w", encoding="utf-8", newline="") as noAlma:
        writer = csv.writer(noAlma)
        writer.writerow(["Scientist", "Link"])
        writer.writerows(has_no_alma)

    with open(Final_Record, "w", encoding="utf-8") as outfile:
        json.dump(list_of_dictionaries, outfile, indent=4, ensure_ascii=False)


# function designed for testing purposes, feel free to ignore!
def test():
    awards_Record = []
    alma_mater_Record = []

    result = requests.get("https://en.wikipedia.org/wiki/Konrad_Zuse")
    doc = BeautifulSoup(result.text, "html.parser")

    # specify the desired position in the soup tree, that of the info table.
    if doc.find(class_="infobox"):
        pos = doc.find(class_="infobox").tbody
        # test = pos.find('th', string = re.compile('Awards')).next_sibling.stripped_strings

        try:
            if pos.find("th", string=re.compile("Alma*")):
                # scope in the Alma mater section of the table and find all the listed universities. Afterwards, iterate through them and capture all titles.
                alma_mater = pos.find(
                    "th", string=re.compile("Alma*")
                ).next_sibling.find_all("a")
                for i in alma_mater:
                    try:
                        alma_mater_Record.append(i["title"])
                    except:
                        pass
            else:
                print("David P. Anderson" + " - alma -")

            # scope in the awards section of the table and find all the listed awards. Afterwards, iterate through them and capture all titles.
            awards = pos.find("th", string=re.compile("Awards")).next_sibling.find_all(
                "a"
            )
            for i in awards:
                try:
                    awards_Record.append(i["title"])
                except:
                    pass
        except:
            print("David P. Anderson" + " - awards")

    else:
        print("Has not table")

    print(awards_Record, "\n", alma_mater_Record)


print("1. Update the list of scientists.\n2. Update info of scientists.\n3. Quit.\n")
option = str(input())

if option == "1":
    Update_Scientists_Record()
elif option == "2":
    Update_Final_Record()
else:
    quit()
