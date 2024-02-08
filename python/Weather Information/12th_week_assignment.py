#DSC 510
#Week 12
#Programming Assignment Week 12
#Author Kabindra Senapati
#11/14/2021
#  Description: This program will get the user the required weather information based on the city or zip code

import requests

# requesting the weather information by city name.
def by_city():

    try:
        city_name = input('Please Enter Your City Name: ')
        state = input('Please enter the state abbreviation: ').lower()
        input_measurment = input('Would you like to view temps in Fahrenheit, Celsius, or Kelvin. . F for Fahrenheit,C for Celsius , K for kelvin: ')
        ##Get the URL and unit of measurement
        url, unit_measurement = get_url('city', input_measurment, None, city_name, state)
        res = requests.get(url)
        data = res.json()
        show_data(data,unit_measurement)
    except ValueError as e:
        print("Not a Valid Entry.. Please try Again",e)
    ##Another search or exit
    exitCondition()

# requesting the weather information by zip code.
def by_zip():

    try:
        zip_code = int(input('Please Enter Your Zip code: '))
        input_measurment = input('Would you like to view temps in Fahrenheit, Celsius, or Kelvin. Enter F for Fahrenheit,C for Celsius,K for kelvin  ')
        ##Get the URL and unit of measurement
        url, unit_measurement = get_url('zip', input_measurment, zip_code)
        res = requests.get(url)
        data = res.json()
        show_data(data,unit_measurement)
    except ValueError as e:
        print("Not a Valid Entry.",e)
    ##Another search or exit
    exitCondition()

def get_url(answer,measurement,zip_code=None,city_name='',state_id=''):
    try:
        if (answer.lower() == 'zip' and measurement.upper() == 'F'):
            unit_measurement = 'Fahrenheit'
            url = 'https://api.openweathermap.org/data/2.5/weather?zip={},us&units=imperial&appid={}'.format(zip_code,apiKey)
            # print(url)
        elif (answer.lower() == 'zip' and measurement.upper() == 'C'):
            unit_measurement = 'Celsius'
            url = 'https://api.openweathermap.org/data/2.5/weather?zip={},us&units=metric&appid={}'.format(zip_code,apiKey)
        elif answer.lower() == 'zip':
            unit_measurement = 'Kelvin'
            url = 'https://api.openweathermap.org/data/2.5/weather?zip={},us&appid={}'.format(zip_code,apiKey)
        elif answer.lower() == 'city' and measurement.upper() == 'F':
            unit_measurement = 'Fahrenheit'
            url = 'https://api.openweathermap.org/data/2.5/weather?q={},{},us&units=imperial&appid={}'.format(city_name,state_id,apiKey)
            # print(url)
        elif answer.lower() == 'city' and measurement.upper() == 'C':
            unit_measurement = 'Celsius'
            url = 'https://api.openweathermap.org/data/2.5/weather?q={},{},us&units=metric&appid={}'.format(city_name,state_id,apiKey)
        elif answer.lower() == 'city':
            unit_measurement = 'Kelvin'
            url = 'https://api.openweathermap.org/data/2.5/weather?q={},{},us&appid={}'.format(city_name,state_id,apiKey)
    except:
        print("Looks like you entered wrong information. Try Again")

    return url,unit_measurement

def exitCondition():
    question = input('Do you want to do another search ? (Y/N): ').lower()
    if question == 'y':
        main()
    else:
        print("Thank you for using the program. Good Bye!")
        exit()

# display the weather information for the weather report.
def show_data(data,unit_measurement):
    temp = data['main']['temp']
    hightemp = data['main']['temp_max']
    lowtemp = data['main']['temp_min']
    press = data['main']['pressure']
    humid = data['main']['humidity']
    city_name = data['name']
    clouds = data['clouds']
    ##Get the cloud coverage based on the percentage of cloud cover
    if clouds['all'] > 75:
            cloud_cover = 'Full Cloudy'
    elif clouds['all'] > 50 and  clouds['all'] < 75:
            cloud_cover = 'Pretty Cloudy'
    elif clouds['all'] > 25 and  clouds['all'] < 50:
            cloud_cover = 'Light Cloudy'
    else:
            cloud_cover = 'No Cloud at all'
    print('Current Weather for {}'.format(city_name))
    print('Current Temp : {} degree {}'.format(temp,unit_measurement))
    print('High Temp : {} degree {}'.format(hightemp,unit_measurement))
    print('Low Temp : {} degree {}'.format(lowtemp,unit_measurement))
    print('Pressure : {} hPa'.format(press))
    print('Humidity : {} %'.format(humid))
    print('Cloud cover : {}'.format(cloud_cover))

# defining main function to run the program
def main():

    if welcome == 'x':
        print("Exiting the program. Thank you")
        exit()
    while True:
        answer = int(input("Would you like to lookup weather data by US City or zip code? Enter 1 for US City, 2 for zip : "))
        if answer == 1:
            try:
                print("Connection established.")
                by_city()
            except Exception:
                print("You did not enter a valid name. Try again")
                by_city()
        elif answer == 2:

            try:
                print("Connection established.")
                by_zip()
            except Exception:
                print("You did not enter a valid zip code numbers. Try again")
                by_zip()
        else:
            print("well, that is not one of the options. Try again.")

if __name__ == '__main__':
    welcome = input("Welcome to the Weather Report Program: Press Any Key to Continue or 'X' to Exit ").lower()
    apiKey = 'ebf52c7bf09bfe3fdeea0aec341fae7f'
    main()