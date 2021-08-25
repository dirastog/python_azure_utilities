from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ResourceNotFoundError


class KeyvaultSecretsUtility:
    '''
    Interface to the Azure KeyVault Python SDK. This class
    helps to get a keyvault client by passing spn_creds
    (spn_id, spn_password, tenant_id) and keyvault name
    '''
    def __init__(self, spn_credentials, kv_name):
        self.client = None
        self.spn_client_id = spn_credentials['spn_id']
        self.spn_client_secret = spn_credentials['spn_password']
        self.tenant_id = spn_credentials['tenant_id']
        self.keyvault_name = kv_name
        self.url = f'https://{self.keyvault_name}.vault.azure.net/'
        self.client = self._get_client()

    def _get_credential(self):
        '''
        This method gets a ClientSecretCredential token
        using spn_credentials (tenant_id, spn_client_id, spn_client_secret)
        '''
        return ClientSecretCredential(
            self.tenant_id,
            self.spn_client_id,
            self.spn_client_secret)

    def _get_client(self):
        '''
        This method is used to create a secret cclient using the kv url
        ClientSecretCredential
        '''
        credential = self._get_credential()
        return SecretClient(
            vault_url=self.url,
            credential=credential)

    def get_secret(self, secret_name):
        '''
        This method is used to fetch the secret value using thing
        secret client
        '''
        try:
            secret = self.client.get_secret(secret_name)
            return secret.name, secret.value
        except ResourceNotFoundError as err:
            msg = 'Secret {} not found \n {}'.format(secret_name, err)
            raise ResourceNotFoundError(msg)
