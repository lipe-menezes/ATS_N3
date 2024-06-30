import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;



import org.json.JSONArray;
import org.json.JSONObject;

public class WeatherCollector {

    private static final String API_URL = "https://api.open-meteo.com/v1/forecast";
    private static final int NUM_CITIES = 27;
    private static final int NUM_DAYS = 31;

    private static final String[][] CITIES = {
        {"Aracaju", "-10.9167", "-37.05"},
        {"Belém", "-1.4558", "-48.5039"},
        {"Belo Horizonte", "-19.9167", "-43.9333"},
        {"Boa Vista", "-2.81972", "-60.67333"},
        {"Brasília", "-15.7939", "-47.8828"},
        {"Campo Grande", "-20.44278", "-54.64639"},
        {"Cuiabá", "-15.5989", "-56.0949"},
        {"Curitiba", "-25.4297", "-49.2711"},
        {"Florianópolis", "-27.5935", "-48.55854"},
        {"Fortaleza", "-3.7275", "-38.5275"},
        {"Goiânia", "-16.6667", "-49.25"},
        {"João Pessoa", "-7.12", "-34.88"},
        {"Macapá", "0.033", "-51.05"},
        {"Maceió", "-9.66583", "-35.73528"},
        {"Manaus", "-3.1189", "-60.0217"},
        {"Natal", "-5.7833", "-35.2"},
        {"Palmas", "-10.16745", "-48.32766"},
        {"Porto Alegre", "-30.0331", "-51.23"},
        {"Porto Velho", "-8.76194", "-63.90389"},
        {"Recife", "-8.05", "-34.9"},
        {"Rio Branco", "-9.97472", "-67.81"},
        {"Rio de Janeiro", "-22.9111", "-43.2056"},
        {"Salvador", "-12.9747", "-38.4767"},
        {"São Luís", "-2.5283", "-44.3044"},
        {"São Paulo", "-23.55", "-46.6333"},
        {"Vitória", "-20.2889", "-40.3083"}
    };

    public static void main(String[] args) {
        // Executar os quatro experimentos
        runExperiment(1);  // Experimento com 1 thread
        runExperiment(3);  // Experimento com 3 threads
        runExperiment(9);  // Experimento com 9 threads
        runExperiment(27); // Experimento com 27 threads
    }

    private static void runExperiment(int numThreads) {
        long totalTime = 0;

        for (int i = 0; i < 10; i++) {
            long startTime = System.currentTimeMillis();

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Callable<Void>> tasks = new ArrayList<>();

            for (String[] city : CITIES) {
                tasks.add(new CityWeatherTask(city[0], city[1], city[2]));
            }

            try {
                List<Future<Void>> results = executor.invokeAll(tasks);
                for (Future<Void> result : results) {
                    result.get(); // Ensure all tasks are completed
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            executor.shutdown();
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long endTime = System.currentTimeMillis();
            totalTime += (endTime - startTime);

            // Aqui você pode adicionar a lógica para processar e exibir os dados coletados, se necessário
        }

        System.out.println("Average time for " + numThreads + " threads: " + (totalTime / 10.0) + " ms");
    }

    private static class CityWeatherTask implements Callable<Void> {
        private String cityName;
        private String latitude;
        private String longitude;

        public CityWeatherTask(String cityName, String latitude, String longitude) {
            this.cityName = cityName;
            this.latitude = latitude;
            this.longitude = longitude;
        }

        @Override
        public Void call() {
            try {
                String response = fetchWeatherData(latitude, longitude);
                processWeatherData(response, cityName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        private String fetchWeatherData(String latitude, String longitude) throws Exception {
            String urlString = String.format("%s?latitude=%s&longitude=%s&hourly=temperature_2m&start=2024-01-01&end=2024-01-31", API_URL, latitude, longitude);
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }

            in.close();
            conn.disconnect();

            return content.toString();
        }

        private void processWeatherData(String data, String cityName) {
            JSONObject jsonObject = new JSONObject(data);
            JSONArray temperatures = jsonObject.getJSONObject("hourly").getJSONArray("temperature_2m");
            int hoursPerDay = 24;

            for (int day = 0; day < NUM_DAYS; day++) {
                double minTemp = Double.MAX_VALUE;
                double maxTemp = Double.MIN_VALUE;
                double sumTemp = 0;

                for (int hour = 0; hour < hoursPerDay; hour++) {
                    double temp = temperatures.getDouble(day * hoursPerDay + hour);
                    if (temp < minTemp) {
                        minTemp = temp;
                    }
                    if (temp > maxTemp) {
                        maxTemp = temp;
                    }
                    sumTemp += temp;
                }

                double avgTemp = sumTemp / hoursPerDay;
                System.out.printf("City: %s, Day: %d, Min: %.2f, Max: %.2f, Avg: %.2f%n", cityName, day + 1, minTemp, maxTemp, avgTemp);
            }
        }
    }
}