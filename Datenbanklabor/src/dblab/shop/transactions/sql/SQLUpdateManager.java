package dblab.shop.transactions.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC Aufgabe 4d
 * 
 * Aktualisieren des Datenbankschemas.
 *
 * Das von den hska_*_bike.sql Skripten erzeugte Tabellen-Schema soll aktualisiert,
 * d.h. erweitert, werden. Vor dem Lösen der Aufgabe sollte zusätzlich das passende
 * hska_*_bike2.sql Skript ausgeführt werden, um zusätzliche Datensätze zu
 * erzeugen.
 * 
 * Die Aufgabe ist, Redundanzen in der Tabelle Teilestamm, die in der Spalte 'farbe' zu finden sind, 
 * zu vermeiden. Dazu soll eine neue Tabelle 'farbe' angelegt werden. 
 * Die Tabelle Teilestamm soll dann für die neue Tabelle 'farbe' einen 
 * Fremdschlüssel verwenden.
 * Bei dieser Gelegenheit werden die Farben in der Tabelle 'farbe' um
 * zusätzliche Informationen ergänzt.
 * 
 * Die Tabelle 'farbe' bekommt das folgende physikalische Datenmodell:
 * 
 * 	Spalte	Beschreibung
 * 	nr		Der Type ist INTEGER, automatisches Hochzählen ist erlaubt, aber
 *  		nicht notwendig (es gibt leider keine einheitliche Syntax, die zwischen Oracle, MySQL
 *  		und PostgreSQL kompatibel ist). Diese Spalte bildet den
 *  		Primärschlüssel.
 *	name	Hat die gleichen Eigenschaften wie 'teilestamm.farbe', aber die Spalte
 *			darf keine Duplikate enthalten.
 *	rot, gruen, blau
 *			Diese Spalten sind vom Typ REAL in einem Wertebereich von
 *			[0.0; 1.0], der sichergestellt werden muss.
 *			Der Standardwert ist 0.
 * 
 * Test Ausgabe
 * 
 * Die folgende Ausgabe sollte auf System.out erscheinen, wenn die main()
 * Methode zum ersten mal aufgerufen wird:
 * Updating database layout ...
 * Table 'farbe' created.
 * Added 3 rows to 'farbe'
 * Column 'farbnr' added to table 'teilestamm'
 * Set 'teilestamm.farbnr' in 34 rows
 * Column 'farbe' removed from 'teilestamm'
 */
public class SQLUpdateManager implements SQLConnectorClient {

	/**
	 * Die verwendete SQL Verbindung.
	 */
	private Connection connection;
	
	/**
	 * Der Konstruktor löst den Update-Vorgang aus.
	 * 
	 * @param connector
	 *            Eine SQLConnector Instanz
	 * @throws SQLException
	 *             Wird geworfen, wenn die Datenbankverbindung oder ein
	 *             Statement scheitert
	 * @throws UpdateFailedException
	 *             Wird geworfen, falls das Update scheitert
	 */
	public SQLUpdateManager(SQLConnector connector) throws SQLException, UpdateFailedException {
		
		this.connection = connector.getConnection(this);

		if (!hasTable("farbe")) {
			update();
		}
		else
		{
			String err = "Table 'farbe' already created!";
			System.err.println( err );
			throw new UpdateFailedException( err ); 
		}
	}

	/**
	 * Prüft, ob eine Tabelle existiert.
	 * 
	 * @param table
	 * 		Die zu prüfende Tabelle
	 * @return
	 * 		True, falls die Tabelle existiert, sonst False
	 * @throws SQLException
	 * 		Im Fall von Verbindungsproblemen
	 */
	private boolean hasTable(String table) throws SQLException {
		DatabaseMetaData dmd = connection.getMetaData();
		ResultSet rs = dmd.getTables(null, null, table, new String[] {"TABLE"});
		boolean hasTable = rs.next();
		rs.close();
		return hasTable;
	}

	/**
	 * Aktualisiere das Datenbanklayout.
	 *
	 * Führt die folgenden Aktionen aus:
	 * 		- Sicherstellen, dass der Update in *einer* Transaktion läuft, nicht in vielen.
	 * 		- Tabelle farbe anlegen
	 * 		- Vorhandene Farben von teilestamm.farbe in farbe.name kopieren
	 * 		- RGB Werte zu farbe Einträgen setzen
	 * 		- In teilestamm die Spalte farbnr (als Foreign Key) anlegen
	 * 		- Die Spalte teilestamm.farbnr mit Werten befüllen
	 * 		- Die Spalte teilestamm.farbe entfernen
	 * 		- Im Erfolgsfall Änderungen committen, sonst zurückrollen
	 * 		- Die ursprüngliche Isolationsebene wiederherstellen
	 * @throws SQLException
	 * 		Im Fall von Verbindungsproblemen
	 * @throws UpdateFailedException
	 * 		Wird geworfen, falls das Update scheitert
	 */
	private void update() throws SQLException {
		System.out.println("Updating database layout ...");

		//Prepare Transaction
		connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		connection.setAutoCommit(false);
		
		//Neue Tabelle anlegen
		Statement stmt = connection.createStatement();
		stmt.executeUpdate("CREATE TABLE farbe(nr INT, name character(10) UNIQUE NOT NULL, rot REAL CHECK(rot >= 0.0 AND rot <= 1.0) DEFAULT 0, gruen REAL CHECK(gruen >= 0.0 AND rot <= 1.0) DEFAULT 0, blau REAL CHECK(blau >= 0.0 AND rot <= 1.0) DEFAULT 0, PRIMARY KEY (nr))");
		System.out.println("Table 'farbe' created");
		
		//Farben in neue Tabelle kopieren
		stmt = connection.createStatement();
		int affected = stmt.executeUpdate("INSERT INTO farbe (name) SELECT farbe FROM teilestamm WHERE farbe IS NOT NULL GROUP BY farbe ORDER BY farbe");
		System.out.println(affected + " rows added to 'farbe'");
		
		//RGB Werte in neue Tabelle eintragen
		stmt = connection.createStatement();
		stmt.addBatch("UPDATE farbe SET rot = 0.7, gruen = 0.3, blau = 0.2 WHERE name = 'schwarz';");
		stmt.addBatch("UPDATE farbe SET rot = 1.0, gruen = 0.0, blau = 0.0 WHERE name = 'rot';");
		stmt.addBatch("UPDATE farbe SET rot = 0.0, gruen = 0.0, blau = 1.0 WHERE name = 'blau';");	
		int[] affectedBatch = stmt.executeBatch();
		int sum = 0;
		for (int i=0; i < affectedBatch.length; i++){
		    sum += affectedBatch[i];
		  }
		System.out.println("Updated " + sum + "rows.");
		
		//In teilestamm die spalate teilnr als FK anlegen
		stmt = connection.createStatement();
		stmt.executeUpdate("ALTER TABLE teilestamm ADD farbnr INT REFERENCES farbe (nr)");
		System.out.println("Column 'farbnr' added to table 'teilestamm'");
		
		//Die Spalte teilestamm.farbnr mit Werten befüllen
		stmt = connection.createStatement();
		stmt.addBatch("UPDATE teilestamm SET farbnr=1 WHERE farbe = 'blau';");
		stmt.addBatch("UPDATE teilestamm SET farbnr=2 WHERE farbe = 'rot';");
		stmt.addBatch("UPDATE teilestamm SET farbnr=3 WHERE farbe = 'schwarz';");
		affectedBatch = stmt.executeBatch();
		sum = 0;
		for (int i=0; i < affectedBatch.length; i++){
		    sum += affectedBatch[i];
		  }
		System.out.println("Set 'teilestamm.farbnr' in " + sum + "rows.");
		
		//Die Spalte teilestamm.farbe entfernen
		stmt = connection.createStatement();
		stmt.executeUpdate("ALTER TABLE teilestamm DROP COLUMN farbe;");
		System.out.println("Column 'farbe' removed from 'teilestamm'");

		connection.commit();
		stmt.close();
	    close();
	}

	/**
	 * Gibt die SQL Ressourcen frei.
	 */
	public void close() throws SQLException {
		connection.close();
	}

	/**
	 * Diese Methode wird zum Testen der Implementierung verwendet.
	 *
	 * @param args
	 *            Kommandozeilenargumente, nicht verwendet
	 * @throws SQLException
	 *             Bei jedem SQL Fehler
	 */
	public static void main(String[] args) throws SQLException, UpdateFailedException {
		SQLConnector bikedb = new SQLConnector();
		new SQLUpdateManager(bikedb);
	}
}
