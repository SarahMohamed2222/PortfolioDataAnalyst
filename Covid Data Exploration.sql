CREATE TABLE CovidDeaths4 (
    iso_code VARCHAR(10),
    continent VARCHAR(50),
    location VARCHAR(100),
    date DATE,
    population FLOAT,
    total_cases FLOAT,
    new_cases FLOAT,
    new_cases_smoothed FLOAT,
    total_deaths FLOAT,
    new_deaths FLOAT,
    new_deaths_smoothed FLOAT,
    total_cases_per_million FLOAT,
    new_cases_per_million FLOAT,
    new_cases_smoothed_per_million FLOAT,
    total_deaths_per_million FLOAT,
    new_deaths_per_million FLOAT,
    new_deaths_smoothed_per_million FLOAT,
    reproduction_rate FLOAT,
    icu_patients FLOAT,
    icu_patients_per_million FLOAT,
    hosp_patients FLOAT,
    hosp_patients_per_million FLOAT,
    weekly_icu_admissions FLOAT,
    weekly_icu_admissions_per_million FLOAT,
    weekly_hosp_admissions FLOAT,
    weekly_hosp_admissions_per_million FLOAT
);

BULK INSERT CovidDeaths4
FROM '/mnt/CovidDeaths.csv'
WITH (
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '\n', 
    FIRSTROW = 2
);

CREATE TABLE covidVaccinations (
    iso_code VARCHAR(10), -- ISO code of the country
    continent VARCHAR(50), -- Continent name
    location VARCHAR(100), -- Country or region name
    date DATE, -- Date of the record
    new_tests FLOAT, -- Number of new tests
    total_tests FLOAT, -- Total number of tests
    total_tests_per_thousand FLOAT, -- Total tests per thousand people
    new_tests_per_thousand FLOAT, -- New tests per thousand people
    new_tests_smoothed FLOAT, -- Smoothed number of new tests
    new_tests_smoothed_per_thousand FLOAT, -- Smoothed new tests per thousand people
    positive_rate FLOAT, -- Positive rate of tests
    tests_per_case FLOAT, -- Tests per case
    tests_units VARCHAR(50), -- Unit of tests reported
    total_vaccinations FLOAT, -- Total number of vaccinations
    people_vaccinated FLOAT, -- Total number of people vaccinated
    people_fully_vaccinated FLOAT, -- Total number of fully vaccinated people
    new_vaccinations FLOAT, -- New vaccinations
    new_vaccinations_smoothed FLOAT, -- Smoothed new vaccinations
    total_vaccinations_per_hundred FLOAT, -- Total vaccinations per hundred people
    people_vaccinated_per_hundred FLOAT, -- People vaccinated per hundred
    people_fully_vaccinated_per_hundred FLOAT, -- Fully vaccinated people per hundred
    new_vaccinations_smoothed_per_million FLOAT, -- Smoothed new vaccinations per million
    stringency_index FLOAT, -- Stringency index
    population_density FLOAT, -- Population density
    median_age FLOAT, -- Median age of the population
    aged_65_older FLOAT, -- Percentage aged 65 and older
    aged_70_older FLOAT, -- Percentage aged 70 and older
    gdp_per_capita FLOAT, -- GDP per capita
    extreme_poverty FLOAT, -- Percentage of extreme poverty
    cardiovasc_death_rate FLOAT, -- Cardiovascular death rate
    diabetes_prevalence FLOAT, -- Diabetes prevalence
    female_smokers FLOAT, -- Percentage of female smokers
    male_smokers FLOAT, -- Percentage of male smokers
    handwashing_facilities FLOAT, -- Availability of handwashing facilities
    hospital_beds_per_thousand FLOAT, -- Hospital beds per thousand people
    life_expectancy FLOAT, -- Life expectancy
    human_development_index FLOAT -- Human Development Index
);

BULK INSERT covidVaccinations
FROM '/mnt/CovidVaccinations.csv'
WITH (
    FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '\n', 
    FIRSTROW = 2
);

-- We are going to look at number of total cases vs total number of deaths (Deathspercentage)

Select Location, date , total_cases, total_deaths, (total_deaths/total_cases)* 100 as DeathPercentage
From PortflioData..CovidDeaths4
where continent is not null
order by 1,2

--This shows likelihood of dying if we contract corona in Africa/ we can do this instead WHERE CHARINDEX('Africa', Location) > 0
Select Location, date , total_cases, total_deaths, (total_deaths/total_cases)* 100 as DeathPercentage
From PortflioData..CovidDeaths4
where location like '%Africa%'
and continent is not null
order by 1,2

-- Looking at Total Cases vs Population
-- shows what percentage of the population got covid
Select Location, date , total_cases, population, (total_cases/population)* 100 as CovidPercentage
From PortflioData..CovidDeaths4
where location like '%Africa%'
and continent is not null
order by 1,2

--look at countries having the highest infection rate compared to population

Select Location, MAX(total_cases) AS HighestInfectionCount , population, MAX((total_cases/population))* 100 as PercentPopulationInfected
From PortflioData..CovidDeaths4
where continent is not null
Group by Location, population
order by PercentPopulationInfected DESC

-- show countries with highest death count per population
Select Location, MAX(total_deaths) AS TotalDeathCount 
From PortflioData..CovidDeaths4
where continent is not null
Group by Location
order by TotalDeathCount DESC

-- do the same thing but for continents
Select continent, MAX(total_deaths) AS TotalDeathCount 
From PortflioData..CovidDeaths4
where continent is null
Group by continent
order by TotalDeathCount DESC

-- we are getting global numbers 

Select date , sum(new_cases) as total_cases, sum(new_deaths)as total_deaths, sum(new_deaths)/sum(new_cases)*100 as DeathPercentage
From PortflioData..CovidDeaths4
where continent is not null
Group by date
order by 1,2

-- getting total amount of people who are vaccinated
Select *
From PortflioData..CovidDeaths4 deaths
join PortflioData..covidVaccinations vaccinations
   On deaths.location= vaccinations.location
   and deaths.date= vaccinations.date

Select deaths.continent, deaths.location, deaths.date, deaths.population,vaccinations.new_vaccinations
From PortflioData..CovidDeaths4 deaths
join PortflioData..covidVaccinations vaccinations
   On deaths.location= vaccinations.location
   and deaths.date= vaccinations.date
where deaths.continent is not null
order by 2,3



-- USE CTE

With PopvsVac(Continent,Location,Date,Population,new_vaccinations,RollingpeopleVaccinated)
as
(
Select deaths.continent, deaths.location, deaths.date, deaths.population,vaccinations.new_vaccinations,
SUM(vaccinations.new_vaccinations) OVER (Partition by deaths.Location Order by deaths.location
,deaths.Date) as RollingpeopleVaccinated

From PortflioData..CovidDeaths4 deaths
join PortflioData..covidVaccinations vaccinations
   On deaths.location= vaccinations.location
   and deaths.date= vaccinations.date
where deaths.continent is not null
--order by 2,3
)
Select * , (RollingPeopleVaccinated/Population)*100
From PopvsVac



--Temp Table 

DROP Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
new_Vaccinations numeric,
RollingPeopleVaccinated numeric
) 


insert into #PercentPopulationVaccinated
Select deaths.continent, deaths.location, deaths.date, deaths.population,vaccinations.new_vaccinations,
SUM(vaccinations.new_vaccinations) OVER (Partition by deaths.Location Order by deaths.location
,deaths.Date) as RollingpeopleVaccinated

From PortflioData..CovidDeaths4 deaths
join PortflioData..covidVaccinations vaccinations
   On deaths.location= vaccinations.location
   and deaths.date= vaccinations.date
where deaths.continent is not null
--order by 2,3

Select *, (RollingPeopleVaccinated/Population)*100
From #PercentPopulationVaccinated




-- Create View to visualize later
GO;
Create View PercentPopulationVaccinated5 as
Select deaths.continent, deaths.location, deaths.date, deaths.population,vaccinations.new_vaccinations,
SUM(vaccinations.new_vaccinations) OVER (Partition by deaths.Location Order by deaths.location
,deaths.Date) as RollingpeopleVaccinated
From PortflioData..CovidDeaths4 deaths
join PortflioData..covidVaccinations vaccinations
   On deaths.location= vaccinations.location
   and deaths.date= vaccinations.date
where deaths.continent is not null
GO;

Select *
From PercentPopulationVaccinated5
