import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "jumpserver.settings")
django.setup()

from orgs.utils import tmp_to_root_org
from assets.models import Node


def check(qs1, qs2):
    count1 = qs1.count()
    count2 = qs2.count()
    if count1 != count2:
        print(f'count error {count1} {count2}')
        errors = []
        if count1 > count2:
            for _node in qs1:
                if not qs2.filter(id=_node.id).exists():
                    errors.append(_node)
        else:
            for _node in qs2:
                if not qs1.filter(id=_node.id).exists():
                    errors.append(_node)
        for error in errors:
            parent_node = Node.objects.get(key=error.parent_key)
            print('-----------------------')
            print(f'{error.left} {error.right} {error.key} {error.parent_key}')
            print(f'{parent_node.left} {parent_node.right} {parent_node.key}')
            print('-----------------------')
        return False

    for _node in qs1:
        if not qs2.filter(id=_node.id).exists():
            print(f'{_node} not in')
            return False
    return True


@tmp_to_root_org()
def verify_nodes():
    nodes = Node.objects.exclude(key__startswith='-')

    for node in nodes:
        ancestors = node.ancestors
        mptt_ancestors = node.mptt_ancestors

        all_children = node.all_children
        mptt_all_children = node.mptt_all_children

        if check(ancestors, mptt_ancestors) and check(all_children, mptt_all_children):
            print(f'ok {node}')
        else:
            print(f'error: {node}')
            return


@tmp_to_root_org()
def verify_nodes_parent_key():
    nodes = Node.objects.exclude(key__startswith='-')
    for _node in nodes:
        parent_key = _node.compute_parent_key()
        old_parent_key = _node.parent_key
        if parent_key != old_parent_key:
            _node.parent_key = parent_key
            _node.save()
            print(f'{_node} parent_key error {parent_key} -> {old_parent_key}')


@tmp_to_root_org()
def to_mptt():
    root_nodes = list(Node.objects.filter(parent_key='').exclude(key__startswith='-'))

    while root_nodes:
        index = 1
        node: Node = root_nodes.pop()
        root_node = node
        have_left_nodes = []  # 先进后出，有了左值得节点
        blank_nodes = []  # 先进后出，未处理的节点

        while True:
            node.left = index
            node.save()
            index += 1

            children_nodes = node.children

            if children_nodes:
                have_left_nodes.append(node)
                node, *children_nodes = children_nodes
                blank_nodes.extend(children_nodes)
                continue
            else:
                end = False
                while not end:
                    # 该节点没有孩子节点，或者子孙节点已经处理完，开始填写他的右值，并往回处理它的兄弟节点，或者父亲节点
                    node.right = index
                    node.save()
                    print(f'ok {node}={node.left}:{node.right}')
                    index += 1

                    # 先从未处理节点，查看是否有兄弟节点
                    if blank_nodes:
                        may_be_brother_node: Node = blank_nodes[-1]
                        if may_be_brother_node.parent_key == node.parent_key:
                            # 是兄弟节点
                            blank_nodes.pop()
                            node = may_be_brother_node
                            break

                    # 不是兄弟节点，找父节点
                    if not have_left_nodes:
                        # 父节点队列里没有数据了，任务结束
                        end = True
                        continue

                    parent_node = have_left_nodes.pop()
                    if node.parent_key != parent_node.key:
                        raise ValueError
                    node = parent_node
                    continue
                else:
                    break
                continue
        print(f'finish {root_node} {have_left_nodes} {blank_nodes}')


if __name__ == '__main__':
    verify_nodes()
