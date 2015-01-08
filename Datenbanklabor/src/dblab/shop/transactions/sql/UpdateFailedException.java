package dblab.shop.transactions.sql;

import java.sql.SQLException;

/**
 * Wird von SQLUpdateManager geworfen, falls ein Datenbankupdate gescheitert
 * ist und zurückgerollt wurde.
 */
public class UpdateFailedException extends SQLException {

	/**
	 * Der Konstruktor bekommt eine Fehlernachricht als Parameter.
	 * 
	 * @param msg
	 * 		Die zu zeigende Fehlernachricht
	 */
	public UpdateFailedException(String msg) {
		super(msg);
	}
	
	/**
	 * Automatisch generiert.
	 */
	private static final long serialVersionUID = 2L;

}
