from .cache import OrgRelatedCache, IntegerField
from users.models import UserGroup
from assets.models import Node
from perms.models import AssetPermission
from .models import OrganizationMember, Organization


class OrgResourceStatisticsCache(OrgRelatedCache):
    users_amount = IntegerField()
    groups_amount = IntegerField()
    nodes_amount = IntegerField()
    asset_perms_amount = IntegerField()

    def __init__(self, org_id):
        super().__init__()
        self.org_id = org_id

    def get_key_suffix(self):
        return f'<org:{self.org_id}>'

    def get_current_org(self):
        return Organization.get_instance(self.org_id)

    def compute_users_amount(self):
        users_amount = OrganizationMember.objects.values(
            'user_id'
        ).filter(org_id=self.org_id).distinct().count()
        return users_amount

    def compute_groups_amount(self):
        groups_amount = UserGroup.objects.all().distinct().count()
        return groups_amount

    def compute_nodes_amount(self):
        nodes_amount = Node.objects.all().distinct().count()
        return nodes_amount

    def compute_asset_perms_amount(self):
        asset_perms_amount = AssetPermission.objects.all().distinct().count()
        return asset_perms_amount
