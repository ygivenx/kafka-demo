package myapps;

public class ProcessedClaim {
    String account_id;
    String name;
    String address;
    String claim_id;
    float insured_amount;
    boolean is_fraud;

    private boolean is_suspicious(float amount) {
        return amount > 60000;
    }

    ProcessedClaim(String account_id, String name, String claim_id, float insured_amount) {
        this.account_id = account_id;
        this.name = name;
        this.claim_id = claim_id;
        this.insured_amount = insured_amount;
        this.is_fraud = is_suspicious(insured_amount);
    }
}
