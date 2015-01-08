package dblab.shop.transactions.sql.bonus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * In dieser Aufgabe soll ein Deadlock erzeugt werden. Dazu werden
 * aus dieser Klasse 2 Objekte mit unterschiedlichem Parameter erstellt.
 * Einmal mit dem Parameter "Lager", einmal mit dem Parameter "Auftragsposten".
 * Der Parameter, "Key" genannt, zeigt, welche Tabelle vom Thread zuerst behandelt 
 * wird, sobald dieser gestartet wird.
 * 
 * Da aber auch eine Zeile in der jeweils
 * anderen Relation bearbeitet wird, entsteht hier schnell die aus der Vorlesung 
 * bekannte Situation, in der sich 2 Transaktionen gegenseitig wegen einer benötigten 
 * Relation gegenseitig aussperren.
 * 
 * Sie können diese Klasse starten, ohne dass Sie Änderungen vorgenommen haben,
 * die Ausgabe verdeutlicht den vorgesehenen Ablauf der implementierten Threads.
 *
 */
public class DatabaseThread extends Thread {

	/**
	 * Die SQL Connection Instanz.
	 */
	private Connection connection;
	
	/**
	 * Flag, ob der Thread weiterlaufen soll oder nicht. Ein Thread sollte 
	 * niemals einfach so unterbrochen werden. Wenn man einen Thread wegen 
	 * eines Fehlers beenden möchte, benutzt man solch ein Variable, um
	 * den Thread von der weiteren Bearbeitung und von weiteren Schleifendurchläufen,
	 * falls vorhanden, abzuhalten. 
	 */
	private boolean threadRun;
	
	/**
	 * Die PreparedStatement Instanz.
	 */
	private PreparedStatement stmt;
	
	/**
	 * Die Treiberklasse, wird als String dem Konstruktor übergeben.
	 */
	private String driverClass;
	
	/**
	 * Die Datenbank-URL, wird als String dem Konstruktor übergeben.
	 */
	private String databaseURL;
	
	/**
	 * Der Datenbank Benutzer, wird als String dem Konstruktor übergeben.
	 */
	private String user;
	
	/**
	 * Das Passwort zum Datenbank Benutzer, wird als String dem Konstruktor übergeben.
	 */
	private String password;
	
	/**
	 * Der Parameter, der bestimmt, welche Hauptaufgabe der Thread übernimmt.
	 */
	private String key;
	
	/**
	 * Initialisiert das Thread-Objekt mit den benötigten Daten.
	 */
	protected DatabaseThread(String databaseURL, String user, String password,
			String driverClass, String key) {
		// TODO begin
		// TODO end
	}
	
	/**
	 * Lädt den JDBC-Treiber und verbindet sich mit der Datenbank.
	 * Wirft SQLException, falls die Verbindung fehlschlägt. 
	 * Die Methode macht innerhalb einer Transaktion 2 Tabellenzugriffe,
	 * bevor die Verbindung beendet wird. Der Key, der dem Konstruktor
	 * übergeben wurde, bestimmt in welcher Reihenfolge die Tabellenzugriffe
	 * erfolgen sollen.
	 */
	public void run() {
		
		if (this.threadRun) {
			// Treiber Laden und Verbindung herstellen.
			// Im Catch-Block das Flag zum Stoppen des Threads setzen.
			// TODO begin
			// TODO end
		}
		
		if (this.threadRun){
			// AutoCommit ausschalten und Transaktionsmodus auf "serialisierbar" setzen.
			// Im Catch-Block das Flag zum Stoppen des Threads setzen.
			// TODO begin
			// TODO end
		}
		
		// Die beiden Tabellenzugriffe werden, abhängig vom Key, durchgeführt.
		if (this.threadRun) {
			if (key == "Lager") {
				this.processLager();
				// Thread warten lassen. Nach dem ersten Zugriff in der Transaktion beider Threads
				// lassen wir den Thread kurz warten, um sicher zu stellen, dass bei beiden Threads der
				// erste Zugriff erfolgt ist, ehe es weiter geht. Dies ist meistens nicht nötig, um hier
				// einen Deadlock zu erzeugen, aber so wird es nahezu 100% sicher, dass es zu einem Deadlock
				// kommt. Und wer sagt denn, dass der Thread an dieser Stelle nicht etwas tun könnte,
				// das eine Sekunde in Anspruch nimmt.
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.out.println(e);
				}
				this.processAuftragsposten();
			} else if (key == "Auftragsposten") {
				this.processAuftragsposten();
				// Thread warten lassen. Nach dem ersten Zugriff in der Transaktion beider Threads
				// lassen wir den Thread kurz warten, um sicher zu stellen, dass bei beiden Threads der
				// erste Zugriff erfolgt ist, ehe es weiter geht. Dies ist meistens nicht nötig, um hier
				// einen Deadlock zu erzeugen, aber so wird es nahezu 100% sicher, dass es zu einem Deadlock
				// kommt. Und wer sagt denn, dass der Thread an dieser Stelle nicht etwas tun könnte,
				// das eine Sekunde in Anspruch nimmt.
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.out.println(e);
				}
				this.processLager();
			}
		}

		if (this.threadRun){
			// Wurde bei beiden Zugriffen keine Exception geworfen, darf der Thread committen.
			try {
				connection.commit();
				stmt.close();
				connection.close();
				if (key == "Lager") {
					System.out.println("|      Commit      |                  |"
							+ "      Commit      |                  |");
				} else if (key == "Auftragsposten") {
					System.out.println("|                  |      Commit      |"
							+ "                  |      Commit      |");
				}
			} catch (SQLException sqle) {
				this.threadRun = false;
				System.out.println("SQLException: " + sqle.getMessage());
			}
		} else {
			// Wurde bei einem Zugriff eine Exception geworfen, wird ein Rollback ausgeführt.
			try {
				connection.rollback(); 
				stmt.close();
				connection.close();
				if (key == "Lager") {
					System.out.println("|    Rollback      |                  |"
							+ "    Rollback      |                  |");
				} else if (key == "Auftragsposten") {
					System.out.println("|                  |    Rollback      |"
							+ "                  |    Rollback      |");
				}
			} catch (SQLException sqle) {
				this.threadRun = false;
				System.out.println("SQLException: " + sqle.getMessage());
			}
		}

	}
	
	/**
	 * Auf eine Zeile im Lager wird zugegriffen.
	 */
	private void processLager() {
		if (this.threadRun){
			if (key == "Lager") {
				System.out.println("|    Zugriff       |                  |"
						+ "                  |                  |");
			} else if (key == "Auftragsposten") {
				System.out.println("|                  |    Zugriff       |"
						+ "                  |                  |");
			}
			// Auf eine Zeile in Lager zugreifen.
			// Rufen Sie in Ihrem Catch-Block zur SQLException bitte die Methode
			// this.formatException(SQLException) mit der gerade gefangenen 
			// SQLException auf. Diese kümmert sich dann um die Verarbeitung der Exception.
			// Eine weitere Behandlung (wie zum Beispiel Ausgabe) der Exception ist
			// nicht nötig.
			// Im Catch-Block auch das Flag zum Stoppen des Threads setzen.
			// TODO begin
			// TODO end
			if (this.threadRun){
				if (key == "Lager") {
					System.out.println("|  Zugriff erfolgt |                  |"
							+ "                  |                  |");
				} else if (key == "Auftragsposten") {
					System.out.println("|                  |  Zugriff erfolgt |"
							+ "                  |                  |");
				}
			}
		}
	}
	
	/**
	 * Auf eine Zeile in Auftragsposten wird zugegriffen.
	 */
	private void processAuftragsposten() {
		if (this.threadRun){
			if (key == "Lager") {
				System.out.println("|                  |                  |"
						+ "    Zugriff       |                  |");
			} else if(key == "Auftragsposten") {
				System.out.println("|                  |                  |"
						+ "                  |    Zugriff       |");
			}
			// Auf eine Zeile in Auftragsposten zugreifen.
			// Rufen Sie in Ihrem Catch-Block zur SQLException bitte die Methode
			// this.formatException(SQLException) mit der gerade gefangenen 
			// SQLException auf. Diese kümmert sich dann um die Verarbeitung der Exception.
			// Eine weitere Behandlung (wie zum Beispiel Ausgabe) der Exception ist
			// nicht nötig.
			// Im Catch-Block auch das Flag zum Stoppen des Threads setzen.
			// TODO begin
			// TODO end
			if (this.threadRun){
				if (key == "Lager") {
					System.out.println("|                  |                  |"
							+ "  Zugriff erfolgt |                  |");
				} else if (key == "Auftragsposten") {
					System.out.println("|                  |                  |"
							+ "                  |  Zugriff erfolgt |");
				}
			}
		}
	}
	
	/**
	 * Methode zur Formatierung und Ausgabe der Exception bei erwartetem Deadlock. 
	 * Diese sollte von beiden "process" Methoden im Fall einer SQLException 
	 * aufgerufen werden.
	 */
	private void formatException(SQLException e) {
		String[] temp = e.getMessage().split("\n"); 
		String[] temp2 = this.databaseURL.split(":");
		if (key =="Lager") {
			if( (temp2[1].equals("oracle") && e.getErrorCode()==60) 
					|| (temp2[1].equals("sqlserver") && e.getErrorCode() == 1205)
					|| (temp2[1].equals("mysql") && e.getErrorCode() == 1213) 
					|| (temp2[1].equals("postgresql") 
							&& temp[0].equals("FEHLER: Verklemmung (Deadlock) entdeckt")) ) {
				System.out.println("|   Blockiert T2   |                  |"
						+ "    Deadlock      |   Blockiert T1   |");
			} else {
				System.out.println(e.getMessage());
			}
		} else if (key == "Auftragsposten") {
			if( (temp2[1].equals("oracle") && e.getErrorCode()==60) 
					|| (temp2[1].equals("sqlserver") && e.getErrorCode() == 1205)
					|| (temp2[1].equals("mysql") && e.getErrorCode() == 1213) 
					|| (temp2[1].equals("postgresql") 
							&& temp[0].equals("FEHLER: Verklemmung (Deadlock) entdeckt")) ) {
				System.out.println("|   Blockiert T2   |    Deadlock      |"
						+ "                  |   Blockiert T1   |");
			} else {
				System.out.println(e.getMessage());
			}
		}
	}
	
	/**
	 * Die Main Methode zum Testen der Implementierung. Hier werden 2
	 * Threads erzeugt und gestartet. Sie müssen noch die fehlenden 
	 * Daten für die Datenbankverbindung ergänzen.
	 * 
	 */
	public static void main(String[] args) {
		
		// TODO begin
		DatabaseThread databasethread1 = new DatabaseThread(
		"",
		"", "", "", 
		"Lager");
		DatabaseThread databasethread2 = new DatabaseThread(
		"",
		"", "", "",
		"Auftragsposten");
		// TODO end

		System.out.println("---------------------------------------"
				+ "--------------------------------------");
		System.out.println("|            Zeile in Lager           |"
				+ "       Zeile in Auftragsposten       |");
		System.out.println("---------------------------------------"
				+ "--------------------------------------");
		System.out.println("|    Thread1       |    Thread2       |"
				+ "    Thread1       |    Thread2       |");
		System.out.println("---------------------------------------"
				+ "--------------------------------------");
		
		databasethread1.start();
		databasethread2.start();
		
	}
}
