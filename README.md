# Opis zadania

## Zadanie

Hipotetyczny operator telekomunikacyjny dogadał się ze związkiem banków, że będzie zapobiegał otrzymywaniu phishingowych wiadomości tekstowych przez abonentów sieci, którzy wyrazili na to chęć. Celem jest stworzenie prostego rozwiązania, które będzie służyło temu celowi.

## Założenia

- Operator przechowuje SMS-y w postaci JSON. Przykładowy rekord:
  ```json
  {
    "sender": "234100200300",
    "recipient": "48700800999",
    "message": "Dzień dobry. W związku z audytem nadzór finansowy w naszym banku proszą o potwierdzanie danych pod adresem: https://www.m-bonk.pl.ng/personal-data"
  }
  ```
- Rozwiązanie powinno obsługiwać wszystkie SMS-y, wykrywać phishing i odrzucać takie przypadki.
- Do oceny, czy URL wskazuje na stronę phishingową, należy użyć zewnętrznego serwisu dostępnego po HTTP: [Google Web Risk API](https://cloud.google.com/web-risk/docs/reference/rest/v1eap1/TopLevel/evaluateUri). Serwis jest płatny za każde wywołanie. Pełna obsługa interfejsu nie jest wymagana – token autoryzacyjny może być parametrem konfiguracyjnym.
- Użytkownicy wyrażają chęć skorzystania lub rezygnacji z usługi przez wysłanie SMS o treści `START` lub `STOP` na określony numer.

## Wymagania techniczne

- Rozwiązanie należy stworzyć w dowolnym języku JVM (Java/Kotlin/Scala).
- Kod źródłowy powinien być dostępny w publicznym repozytorium GitHub.
- Rozwiązanie powinno budować obraz(y) wykonywalny(e) Docker, przechowywany(e) w serwisie Docker Hub, wraz z krótkim opisem uruchomienia.
- Cały kod należy umieścić w nowym pull request (dla ułatwienia review).

## Dodatkowe informacje

- Jeśli przyjęto dodatkowe założenia, należy je opisać w pliku README.
- Jednym z ocenianych elementów jest przyjęta architektura rozwiązania. Decyzje architektoniczne należy opisać w README.
- Operatorowi zależy na szybkim czasie wdrożenia i niskim koszcie obsługi.
- W razie wątpliwości należy przyjąć prostszą funkcjonalność, ale przedstawić rozwiązanie jak najbardziej gotowe do wdrożenia produkcyjnego.
