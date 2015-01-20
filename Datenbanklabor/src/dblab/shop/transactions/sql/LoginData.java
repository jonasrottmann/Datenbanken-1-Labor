package dblab.shop.transactions.sql;


/**
 * Die Klasse LoginData stellt die Informationen bereit, die JDBC
 * benötigt, um eine Verbindung mit der Datenbank her zu stellen.
 * 
 * Diese Informationen können über die Getter Methoden abgerufen werden.
 * 
 * Wie sich die "Connection URL" zusammensetzt, zu der Datenbank, die Sie 
 * benutzen, können Sie in der jeweiligen Dokumentation des 
 * verwendeten Datenbanktreibers finden.
 * 
 * Diese finden Sie im Internet z.B. auf der Website der Datenbank.
 * 
 * Aus diversen Gründen macht es keinen Sinn, die Verbindungsinformationen
 * direkt im Quellcode an zu geben. Da es aber verschiedene Möglichkeiten gibt, diese
 * Informationen bereit zu stellen und um den Umfang der Aufgabe nicht zu sprengen, sei
 * es hier erlaubt, den Klassenvariablen diese Informationen direkt im Konstruktor 
 * zuzuweisen.
 * 
 * Sie können aber als erweiterte Übung gerne XML- oder
 * Property Dateien benutzen. 
 * Auch der Einsatz von Programm-Argumenten (d.h. argv von main())
 * oder Umgebungsvariablen sind hier in der Praxis üblich. 
 * 
 */
public class LoginData {
		
	/**
	 * Die Connection URL zur Datenbank.
	 */
	private String databaseURL = null;
	
	/**
	 * Der Benutzername für die Datenbank.
	 */
	private String user = null;
	
	/**
	 * Das Passwort zum Benutzernamen für die Datenbank.
	 */
	private String pass = null;
	
	/**
	 * Erstellt ein LoginData Objekt und legt die Informationen in 
	 * den Klassenvariablen ab. Für die einfache Lösung weisen Sie der jeweiligen
	 * Variable einfach den richtigen Wert zu.
	 * 
	 * @throws Exception
	 * 				Sollten Sie eine Lösung z.B. über Property Datei wählen, können Exceptions
	 * 				beim Laden der Datei oder beim Laden der Property Daten aus dem InputStream
	 * 				geworfen werden.
	 */
	protected LoginData() throws Exception {
		
		//Server
		this.databaseURL = "jdbc:oracle:thin:@iwi-lkit-db-01:1521:lab1";
		this.user = "dbprax60";
		this.pass = "dbprax60";
	}
	
	/**
	 * Gibt die Connection URL für die Verbindung zu Datenbank zurück.
	 * 
	 * @return Die Connection URL
	 */
	public String getDatabaseURL() {
		return this.databaseURL;
	}
	
	/**
	 * Gibt den Benutzernamen für die Verbindung zur Datenbank zurück.
	 * 
	 * @return Den Benutzernamen
	 */
	public String getUser() {
		return this.user;
	}
	
	/**
	 * Gibt das Passwort für die Verbindung zur Datenbank zurück.
	 * 
	 * @return Das Passwort
	 */
	public String getPass() {
		return this.pass;
	}
}
