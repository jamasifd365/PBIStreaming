using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Net.Http;
using System.Threading;


namespace PBIStreaming
{
    
    /* 
Generates random wind speed observations and pushes them to a Power BI streaming dataset.

Supply your own values in the Main method on line 24 - most importantly the Power BI push URL. The script will not run without it.
On lines 19-20 you can set the script duration and data push frequency.
*/

    class Program
    {
        // Use class-level Random for best results
        static Random random = new Random();

        // Use class-level HttpClient as a best practice
        static HttpClient client = new HttpClient();
        static List<WeatherStation> weatherStation;

        static void Main()
        {
            const int duration = 60; // Length of time in minutes to push data
            const int pauseInterval = 2; // Frequency in seconds that data will be pushed
            const string timeFormat = "yyyy-MM-ddTHH:mm:ss.fffZ"; // Time format required by Power BI

            weatherStation = new List<WeatherStation>();
            weatherStation.Add(new WeatherStation { stationID = 1, stationName = "Vancouver Hill Station", powerBiPostUrl = "https://api.powerbi.com/beta/1e4e5d60-28e4-48e0-b1aa-97a2ca6b926e/datasets/4436dbcf-a664-44be-adfe-f029ba6e43b7/rows?noSignUpCheck=1&key=yMywItG%2B8DxjU3Ym3omNV0CaspUOL1%2FATeVJqNmAOsu1W8rqL%2BwfpGDnCVP4vibglT1Pqi7ozctG5Y3WmI4ByQ%3D%3D" });

            GenerateObservations(duration, pauseInterval, timeFormat);
        }

        public static void GenerateObservations(int duration, int pauseInterval, string timeFormat)
        {
            DateTime startTime = GetDateTimeUtc();
            DateTime currentTime;
            do
            {
                foreach (var station in weatherStation)
                {
                    WindSpeedRecording windSpeedRecording = new WindSpeedRecording
                    {
                        stationID = station.stationID,
                        stationName = station.stationName,
                        windSpeed = GenerateRandom(10),
                        dateTime = GetDateTimeUtc().ToString(timeFormat)
                    };

                    var jsonString = JsonConvert.SerializeObject(windSpeedRecording);
                    var postToPowerBi = HttpPostAsync(station.powerBiPostUrl, "[" + jsonString + "]"); // Add brackets for Power BI
                    Console.WriteLine(jsonString);
                }
                Thread.Sleep(pauseInterval * 1000); // Pause for n seconds. Not highly accurate.
                currentTime = GetDateTimeUtc();
            } while ((currentTime - startTime).TotalMinutes <= duration);
        }

        public class WindSpeedRecording
        {
            public int stationID { get; set; }
            public string stationName { get; set; }
            public int windSpeed { get; set; }
            public string dateTime { get; set; }
        }

        public class WeatherStation
        {
            public int stationID { get; set; }
            public string stationName { get; set; }
            public string powerBiPostUrl { get; set; }
        }

        static int GenerateRandom(int max)
        {
            int n = random.Next(max);
            return n;
        }

        static async Task<HttpResponseMessage> HttpPostAsync(string url, string data)
        {
            // Construct an HttpContent object from StringContent
            HttpContent content = new StringContent(data);
            HttpResponseMessage response = await client.PostAsync(url, content);
            response.EnsureSuccessStatusCode();
            return response;
        }

        static DateTime GetDateTimeUtc()
        {
            return DateTime.UtcNow;
        }
    }

}
