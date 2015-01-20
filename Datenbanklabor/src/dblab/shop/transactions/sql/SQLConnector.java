package dblab.shop.transactions.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

/**
 * JDBC Aufgabe 4a
 * 
 * Das Ziel dieser Übung ist es, eine erste JDBC Verbindung herzustellen. Um
 * dieses Ziel zu erreichen, muss die vorliegende Klasse sowie die Klasse
 * LoginData vervollständigt werden. Dies kann durch Ausführen der main()
 * Methode getestet werden.
 *
 * Alle zum Lösen dieser Aufgabe benötigten Informationen können in der
 * offiziellen Java SE 6 Dokumentation bzw. im Skript zur Vorlesung und in der
 * Dokumentation zu der verwendeten Datenbanken gefunden werden.
 *
 * Zur Lösung sollten nur an den markierten Stellen fehlende Codezeilen ergänzt
 * werden. Eine Änderung des Klassen-Interface ist nicht gestattet.
 * 
 * Jede Code-Lücke ist mit einem TODO markiert. Wo nicht vorgegeben, werden
 * unabhängig von der Sichtbarkeit, JavaDoc Kommentare für alle Attribute und
 * Methoden erwartet. Wo es sinnvoll erscheint, sollte auch direkt im Code
 * kommentiert werden.
 *
 * Die nötigen JDBC Treiber für PostgreSQL, MySQL, MSSQL und Oracle liegen
 * diesem Aufgabenpaket bei. Andernfalls finden Sie die Datenbanktreiber für
 * Java Anwendungen auf der Website der jeweiligen Datenbank.
 * 
 * Wie sie die Quelldateien und die Treiber in ein Projekt einbinden können ist
 * im Abschnitt "Tipps" im Skript beschrieben.
 *
 * Datenbanken müssen entweder mit hska_oracle_bike.sql, hska_mysql_bike.sql,
 * hska_mssql_bike oder hska_pgsql_bike.sql initialisiert werden. Wege, die
 * Skripte auszuführen, sind in den Dokumentationen der Datenbanken zu finden.
 * 
 * Ehe Sie mit der Arbeit an dieser Klasse beginnen, sollten sie wissen, wie die
 * Klasse LoginData die Informationen zum Verbinden mit der Datenbank
 * bereitstellt. Natürlich müssen Sie dann auch dafür sorgen, dass LoginData die
 * richtigen Informationen bereitstellt.
 * 
 * Test Ausgabe
 * 
 * Die folgenden Ausgaben werden erwartet, wenn eine Datenbank verfügbar ist:
 * Connection successfully established. Connection successfully closed.
 */
public class SQLConnector {

	/**
	 * Die Login Daten für die Datenbankverbindung
	 */
	private LoginData loginData;
	/**
	 * Die SQL Connection Instanz.
	 */
	private Connection connection;

	/**
	 * Benutzt, um die registrierten Nutzerobjekte zu sammeln.
	 */
	private ArrayList<SQLConnectorClient> clients;

	/**
	 * Baut eine Datenbankverbindung auf.
	 * 
	 * Führt die folgenden Aktionen aus: - Erstellt ein LoginData Objekt. - Lädt
	 * die Klasse des verwendeten Datenbanktreibers. (siehe Dokumentation des
	 * Datenbanktreibers) - Erstellt eine Verbindung mit den Daten aus
	 * LoginData. - Initialisiert die Nutzerobjekte.
	 *
	 * @throws SQLException
	 *             Wird im Falle eines gescheiterten Verbindungsaufbaus geworfen
	 */
	protected SQLConnector() throws SQLException {
		try {
			loginData = new LoginData();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Get URL from LoginData
		String url ="";
		url = loginData.getDatabaseURL();
		

		// Login Data:
		Properties props = new Properties();
		props.put("user", loginData.getUser());
		props.put("password", loginData.getPass());

		// Connect to the database at that URL.
		connection = DriverManager.getConnection(url, props);
		
		//Initialisiert die Nutzerobjekte.
		clients = new ArrayList<SQLConnectorClient>();
	}

	/**
	 * Gibt zurück, ob die Datenbankverbindung noch aufgebaut ist.
	 * 
	 * @return True, falls die Verbindung besteht, sonst false
	 * @throws SQLException
	 *             Im Falle eines Zugriffsfehlers
	 */
	public boolean isOpen() throws SQLException {
		return !connection.isClosed();
	}

	/**
	 * Ruft alle Nutzerobjekte zurück (Callback) und schließt dann die
	 * Datenbankverbindung.
	 * 
	 * @throws SQLException
	 *             Im Falle eines Zugriffsfehlers oder wenn die Verbindung
	 *             bereits geschlossen ist
	 */
	public void close() throws SQLException {		
		for( SQLConnectorClient k: clients ) {
			k.close();
		}
		
		connection.close();
	}

	/**
	 * Gibt die SQL Verbindung an Klassen der SQL-Schicht weiter.
	 *
	 * Die Klassen müssen eine Rückrufobjekt anbieten, dessen Interface als
	 * SQLConnectorClient definiert ist. In der Regel ist das Aufrufende Ojbekt
	 * das Rückrufobjekt.
	 * 
	 * @param client
	 *            Das beim Beenden der Verbindung zurückzurufende Objekt
	 * @return Die Connection Instanz
	 */
	protected Connection getConnection(SQLConnectorClient client) {
		clients.add(client);
		return connection;
	}

	/**
	 * Diese Methode wird zum Testen der Implementierung verwendet.
	 *
	 * @param args
	 *            Kommandozeilenargumente, nicht verwendet
	 * @throws SQLException
	 *             Bei jedem SQL Fehler
	 */
	public static void main(String[] args) throws SQLException {
		SQLConnector sqldb = new SQLConnector();

		if (sqldb.isOpen()) {
			System.out.println("Connection successfully established.");
		} else {
			System.err.println("Establishing the connection failed!");
		}

		sqldb.close();

		if (!sqldb.isOpen()) {
			System.out.println("Connection successfully closed.");
		} else {
			System.err.println("Closing the connection failed!");
		}
	}
}
