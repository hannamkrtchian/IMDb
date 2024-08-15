import { DefaultAzureCredential } from '@azure/identity';
import { SecretClient } from '@azure/keyvault-secrets';
import * as dotenv from 'dotenv';

dotenv.config();

const credential = new DefaultAzureCredential();
const url = process.env.VAULT_URL;
const client = new SecretClient(url, credential);

export async function storeSecret(secretName, secretValue) {
    await client.setSecret(secretName, secretValue);
}
