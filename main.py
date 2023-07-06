from client_orientdb import OrientDBClient
import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from dotenv import load_dotenv

load_dotenv()

NUMBER_PEOPLES = 10000

client = OrientDBClient(os.getenv("DATABASE_URL_CONNECTION"),
                        int(os.getenv("DATABASE_PORT_CONNECTION")))
client.open_database(os.getenv("DATABASE_NAME"), os.getenv(
    "DATABASE_USERNAME"), os.getenv("DATABASE_PASSWORD"))


def create_classes():
    client.command_db(f"CREATE CLASS State EXTENDS V")
    client.command_db(f"CREATE CLASS City EXTENDS V")
    client.command_db(f"CREATE CLASS People EXTENDS V")
    client.command_db(f"CREATE CLASS DISTANCE_KM EXTENDS E")
    client.command_db(f"CREATE CLASS HAS_CITY EXTENDS E")
    client.command_db(f"CREATE CLASS LIVES_IN EXTENDS E")


def add_states_to_orientdb():
    states = {
        "AC": "Acre",
        "AL": "Alagoas",
        "AP": "Amapá",
        "AM": "Amazonas",
        "BA": "Bahia",
        "CE": "Ceará",
        "DF": "Distrito Federal",
        "ES": "Espírito Santo",
        "GO": "Goiás",
        "MA": "Maranhão",
        "MT": "Mato Grosso",
        "MS": "Mato Grosso do Sul",
        "MG": "Minas Gerais",
        "PA": "Pará",
        "PB": "Paraíba",
        "PR": "Paraná",
        "PE": "Pernambuco",
        "PI": "Piauí",
        "RJ": "Rio de Janeiro",
        "RN": "Rio Grande do Norte",
        "RS": "Rio Grande do Sul",
        "RO": "Rondônia",
        "RR": "Roraima",
        "SC": "Santa Catarina",
        "SP": "São Paulo",
        "SE": "Sergipe",
        "TO": "Tocantins"
    }

    for abbreviation, name in states.items():
        client.command_db(
            f"CREATE VERTEX State SET abbreviation = '{abbreviation}', name = '{name}', country = 'Brasil'")


def add_city_to_orientdb():
    df = pd.read_csv('people.csv', sep=';')

    unique_rows = df.drop_duplicates(subset='City', keep='first')

    uniques_city = unique_rows[['City', 'State']]

    for city, state in uniques_city.values:
        state_vertex = client.command_db(
            f"SELECT FROM State WHERE abbreviation = '{state}'")
        state_rid = state_vertex[0]._rid
        client.command_db(f"CREATE VERTEX City SET name = '{city}'")
        city_vertex = client.command_db(
            f"SELECT FROM City WHERE name = '{city}'")
        city_rid = city_vertex[0]._rid
        client.command_db(
            f"CREATE EDGE HAS_CITY FROM {city_rid} TO {state_rid}")


def add_people_to_orientdb():
    df = pd.read_csv('people.csv', sep=';')

    rows = df.values.tolist()

    for name, birthday, cpf, cns, rg, email, cell, cep, address, neighborhood, city_name, city_state in rows:
        people_vertex = client.command_db(f"""
            CREATE VERTEX People 
            SET name = "{name}", 
                birthday = "{birthday}", 
                cpf = "{cpf}", 
                cns = "{cns}", 
                rg = "{rg}", 
                email = "{email}", 
                cell = "{cell}", 
                cep = "{cep}", 
                address = "{address}", 
                neighborhood = "{neighborhood}"
        """)

        city_vertex = client.command_db(f"""
            SELECT FROM City 
            WHERE name = "{city_name}"
        """)
        city_rid = city_vertex[0]._rid

        client.command_db(f"""
            CREATE EDGE LIVES_IN 
            FROM {people_vertex[0]._rid} 
            TO {city_rid}
        """)


def add_distance_city_to_orientdb():
    df = pd.read_csv('people.csv', sep=';')

    unique_rows = df.drop_duplicates(subset='City', keep='first')

    uniques_city = unique_rows[['City', 'State']]

    print(uniques_city)

    geolocator = Nominatim(user_agent="my-custom-user-agent")

    for city1 in uniques_city.itertuples(index=False):
        for city2 in uniques_city.itertuples(index=False):
            if city1 != city2:
                city1aux = geolocator.geocode(city1[0] + ", Brazil")
                city2aux = geolocator.geocode(city2[0] + ", Brazil")
                distance = geodesic(
                    (city1aux.latitude, city1aux.longitude), (city2aux.latitude, city2aux.longitude)).km

                city1_vertex = client.command_db(f"""
                    SELECT FROM City 
                    WHERE name = '{city1[0]}'
                """)
                city1_rid = city1_vertex[0]._rid

                city2_vertex = client.command_db(f"""
                    SELECT FROM City 
                    WHERE name = '{city2[0]}'
                """)
                city2_rid = city2_vertex[0]._rid

                client.command_db(f"""
                    CREATE EDGE DISTANCE_KM 
                    FROM {city1_rid} 
                    TO {city2_rid} 
                    SET distance = {distance}
                """)


def scrape_person():
    # create an instance of the Chrome driver
    driver = webdriver.Chrome()

    # access the generator page
    driver.get("https://geradornv.com.br/gerador-pessoas/")
    time.sleep(2)  # wait for the page to load

    # create an empty DataFrame to store the data
    columns = ['Name', 'Birthday', 'CPF', 'CNS', 'RG', 'Email', 'Cellphone', 'CEP', 'Address', 'Neighborhood', 'City',
               'State']
    df = pd.DataFrame(columns=columns)

    while True:  # loop until all data is scraped successfully
        try:
            # loop 50 times to extract data for 50 different people
            for i in range(NUMBER_PEOPLES):
                # find the "Gerar Pessoa" button
                generate_person_btn = driver.find_element(
                    By.ID, "nv-new-generator-people")

                # click the "Gerar Pessoa" button
                generate_person_btn.click()
                time.sleep(1)  # wait for the new person to be generated

                # extract the data from each field
                name = driver.find_element(By.ID, "nv-field-name").text
                birthday = driver.find_element(By.ID, "nv-field-birthday").text
                cpf = driver.find_element(By.ID, "nv-field-cpf").text
                cns = driver.find_element(By.ID, "nv-field-cns").text
                rg = driver.find_element(By.ID, "nv-field-rg").text
                email = driver.find_element(By.ID, "nv-field-email").text
                cell = driver.find_element(By.ID, "nv-field-cellphone").text
                cep = driver.find_element(By.ID, "nv-field-cep").text
                address = driver.find_element(By.ID, "nv-field-street").text
                neighborhood = driver.find_element(
                    By.ID, "nv-field-neighborhood").text
                city = driver.find_element(By.ID, "nv-field-city").text
                state = driver.find_element(By.ID, "nv-field-state").text

                # add the data to the DataFrame
                person = pd.DataFrame(
                    [[name, birthday, cpf, cns, rg, email, cell,
                        cep, address, neighborhood, city, state]],
                    columns=columns)

                df = pd.concat([df, person], ignore_index=True)

            # if all data is scraped successfully, break the while loop
            break
        except:
            # if any exception is raised, print the error message and try the loop again
            print("Error occurred while scraping. Retrying...")
            continue

    # close the browser
    driver.quit()

    # save the data to a CSV file with semicolon separator and UTF-8 encoding
    df.to_csv('people.csv', sep=';', index=False, encoding='utf-8-sig')


if __name__ == '__main__':
    scrape_person()
    create_classes()
    add_states_to_orientdb()
    add_city_to_orientdb()
    add_people_to_orientdb()
    add_distance_city_to_orientdb()
