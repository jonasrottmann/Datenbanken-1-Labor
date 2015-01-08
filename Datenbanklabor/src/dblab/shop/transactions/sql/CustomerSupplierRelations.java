package dblab.shop.transactions.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * JDBC Aufgabe 4c
 * 
 * Diese Klasse bietet Zugriff auf die Zusammenhänge zwischen Kunden und
 * Lieferanten.
 * 
 * Die folgende Ausgabe wird beim Aufruf der main() Methode erwartet:
 *
 *  kunde                          | knr         | lieferant                      | lnr         
 * --------------------------------+-------------+--------------------------------+-------------
 *  Biker Ecke                     |           5 |                                |             
 *  Fahrrad Shop                   |           1 | Firma Gerti Schmidtner         |           1 
 *  Fahrräder Hammerl              |           6 | Firma Gerti Schmidtner         |           1 
 *  Fahrräder Hammerl              |           6 | MSM GmbH                       |           5 
 *  Maier Ingrid                   |           3 |                                |             
 *  Rafa - Seger KG                |           4 | Firma Gerti Schmidtner         |           1 
 *  Rafa - Seger KG                |           4 | Rauch GmbH                     |           2 
 *  Zweirad-Center Staller         |           2 |                                |             
 * (8 rows)
 *
 * "kunde";"knr";"lieferant";"lnr"
 * "Rafa - Seger KG";4;"Firma Gerti Schmidtner";1
 * "Rafa - Seger KG";4;"Rauch GmbH";2
 *
 *
 *
 * Diese Fragen müssen bei der Abgabe beantwortet sein:
 *
 * ############################################################################
 * - Warum und in welchen Fällen bieten Prepared Statements Performance-
 *   vorteile gegenüber dynamisch generierten Abfragen?
 *   
 *   Soll ein Statement mit unterschiedlichen Parametern mehrere Male
 *   (z. B. innerhalb einer Schleife) auf dem Datenbanksystem ausgeführt werden,
 *   können Prepared Statements einen Geschwindigkeitsvorteil bringen, da das
 *   Statement schon vorübersetzt im Datenbanksystem vorliegt und nur noch
 *   mit den neuen Parametern ausgeführt werden muss.
 *   
 *   
 * ############################################################################  
 * - Was sind die Sicherheitsvorteile von Prepared Statements gegenüber
 *   dynamisch erzeugten Abfragen?
 *   
 *   Mittels Prepared Statements können SQL-Injections effektiv verhindert werden,
 *   da das Datenbanksystem die Gültigkeit von Parametern prüft,
 *   bevor diese verarbeitet werden.
 *   
 */
public class CustomerSupplierRelations implements SQLConnectorClient {

	/**
	 * Die verwendete SQL Connection.
	 */
	private Connection connection;
	
	/**
	 * Ein Statement, das Lieferanten für Kunden listet.
	 */
	private PreparedStatement stmtKundeLieferanten;
	
	/**
	 * Ermittelt die Datenbankverbindung vom SQLConnector.
	 * 
	 * @param connector
	 *            Eine SQLConnector Instanz
	 * @throws SQLException
	 *             Falls ein Verbindungsaufbau oder ein Statement scheitert
	 */
	public CustomerSupplierRelations(SQLConnector connector) throws SQLException {
		
		this.connection = connector.getConnection(this);
	}

	/**
	 * Gibt ein ResultSet zurück, das alle Zulieferer, von denen Teile für
	 * einen Kunden mit der gegebenen Nummer nachgefragt wurden, auflistet.
	 *
	 * Stellt sicher, dass ein Kunde auch dann gelistet wird, wenn keine
	 * Zulieferer gefunden werden.
	 * 
	 * @param kdNr
	 *            Die Kundennr des Kunden, für den Zulieferer ermittelt
	 *            werden sollen, bei der Nummer 0 wird die Liste für alle
	 *            Kunden erzeugt
	 * @return Zulieferer für einen gegebenen Kunden
	 * @throws SQLException
	 *            Falls das ausgeführe Statement scheitert
	 */
	public ResultSet getKundeLieferanten(int kdNr) throws SQLException {
		
		String sql = "SELECT kunde.name kunde, kunde.nr, lieferant.name lieferant, lieferant.nr lfnr FROM lieferant JOIN lieferung ON lieferant.nr = lieferung.liefnr JOIN auftragsposten ON lieferung.teilnr = auftragsposten.teilnr JOIN auftrag ON auftragsposten.auftrnr = auftrag.auftrnr FULL OUTER JOIN kunde ON auftrag.kundnr = kunde.nr " + (kdNr == 0 ? "" : "WHERE auftrag.kundnr = '" + kdNr + "'") +  " GROUP BY kunde.name, kunde.nr, lieferant.name, lieferant.nr ORDER BY kunde.name";
		stmtKundeLieferanten = connection.prepareStatement(sql);
				
		
		ResultSet rs = stmtKundeLieferanten.executeQuery();
		return rs;
	}

	
	/**
	 * Gibt alle Resourcen frei.
	 */
	public void close() throws SQLException {
		stmtKundeLieferanten.close(); 
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
	public static void main(String[] args) throws SQLException {

		SQLConnector sqldb = new SQLConnector();
		CustomerSupplierRelations csr = new CustomerSupplierRelations(sqldb);

		ResultSet rs = csr.getKundeLieferanten(0);
		Output.printResultTable(rs, System.out);
		rs.close();

		rs = csr.getKundeLieferanten(4);
		Output.resultToCsv(rs, System.out);
		rs.close();
	}

}
