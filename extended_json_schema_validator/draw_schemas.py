#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import datetime
import hashlib
import html
import logging
from typing import cast, NamedTuple, TYPE_CHECKING

from .extend_validator_helpers import (
	refResolver_resolve,
)
from .extensions.fk_check import ForeignKey
from .extensions.pk_check import PrimaryKey

if TYPE_CHECKING:
	from typing import (
		Any,
		ClassVar,
		IO,
		Iterator,
		NamedTuple,
		Mapping,
		MutableMapping,
		MutableSequence,
		MutableSet,
		Optional,
		Sequence,
		Set,
		Tuple,
		Type,
		Union,
	)

	from .extensible_validator import ExtensibleValidator

module_logger = logging.getLogger(__name__)

DECO = {
	"object": "{}",
	"array": "[]",
}


class FKEdge(NamedTuple):
	fromNodeId: str
	mport: str
	toNodeId: str
	tooltip: str


def s_sum(the_str: str) -> str:
	oP = hashlib.sha1()
	oP.update(the_str.encode("utf-8"))
	return oP.hexdigest()


def payloadProcessor(
	kPayload: "Mapping[str, Any]",
) -> "Tuple[Mapping[str, Any], Sequence[str]]":
	kAll = []
	kPoss = [kPayload]
	while len(kPoss) > 0:
		kAll.extend(copy.deepcopy(kPoss))
		kPossNext = []
		for kP in kPoss:
			kPossNext.extend(kP.get("allOf", []))
			kPossNext.extend(kP.get("anyOf", []))
			kPossNext.extend(kP.get("oneOf", []))
			for p_name in ("then", "else"):
				p_pl = kP.get(p_name)
				if p_pl is not None:
					kPossNext.append(p_pl)

		kPoss = kPossNext

	kP = {}
	req = []
	if len(kAll) > 0:
		for kOne in kAll:
			req.extend(kOne.get("required", []))

			kOP_l = list(map(kOne.get, ("properties", "patternProperties")))
			kOPN = kOne.get("propertyNames")
			if kOPN is not None:
				the_name = kOPN.get("pattern")
				if the_name is not None:
					kOP_l.append({the_name: kOPN})

			# Detecting type collisions
			for kOP in kOP_l:
				if isinstance(kOP, dict):
					for kOP_k, kOP_v in kOP.items():
						kOP_ov = kP.get(kOP_k)
						if kOP_ov is None:
							kP[kOP_k] = kOP_v
						else:
							# Saving for recombination
							ot_v = kOP_ov.get("type")
							t_v = kOP_v.get("type")
							kOP_ov.update(kOP_v)
							if ot_v is not None and t_v is not None:
								n_t_v = []
								if isinstance(ot_v, list):
									n_t_v.extend(ot_v)
								else:
									n_t_v.append(ot_v)
								if isinstance(t_v, list):
									n_t_v.extend(t_v)
								else:
									n_t_v.append(t_v)
								kOP_ov["type"] = n_t_v
	return kP, req


def simplePayloadProcessor(kPayload: "Mapping[str, Any]") -> "Mapping[str, Any]":
	kAll = []
	kPoss = [cast("MutableMapping[str, Any]", copy.deepcopy(kPayload))]
	while len(kPoss) > 0:
		kAll.extend(kPoss)
		kPossNext = []
		for kP in kPoss:
			for off in ("allOf", "anyOf", "oneOf"):
				if off in kP:
					kPossNext.extend(kP.pop(off))
			for p_name in ("then", "else"):
				p_pl = kP.get(p_name)
				if p_pl is not None:
					kPossNext.append(p_pl)

		kPoss = kPossNext

	kP = {}
	if len(kAll) > 0:
		# Detecting type collisions
		for kOP in kAll:
			if isinstance(kOP, dict):
				# Saving for recombination
				ot_v = kP.get("type")
				t_v = kOP.get("type")

				pp_proc: "MutableMapping[str, Any]" = {}
				p_v = kP.get("properties")
				if p_v is not None:
					p_vp = simplePayloadProcessor(p_v)
					pp_proc.update(p_vp)
					del kP["properties"]
				else:
					p_vp = None

				pp_v = kP.get("patternProperties")
				if pp_v is not None:
					pp_vp = simplePayloadProcessor(pp_v)
					pp_proc.update(pp_vp)
					del kP["patternProperties"]
				else:
					pp_vp = None

				op_v = kOP.get("properties")
				if op_v is not None:
					op_vp = simplePayloadProcessor(op_v)
					pp_proc.update(op_vp)
					del kOP["properties"]
				else:
					op_vp = None

				opp_v = kOP.get("patternProperties")
				if opp_v is not None:
					opp_vp = simplePayloadProcessor(opp_v)
					pp_proc.update(opp_vp)
					del kOP["patternProperties"]
				else:
					opp_vp = None

				kP.update(kOP)

				# type reconciliation
				if ot_v is not None and t_v is not None:
					n_t_v = []
					if isinstance(ot_v, list):
						n_t_v.extend(ot_v)
					else:
						n_t_v.append(ot_v)
					if isinstance(t_v, list):
						n_t_v.extend(t_v)
					else:
						n_t_v.append(t_v)
					kP["type"] = n_t_v

				# properties reconciliation
				if pp_proc:
					kP["properties"] = pp_proc

	return kP


def genObjectNodes(
	label: str,
	kPayload: "Mapping[str, Any]",
	prefix: "Optional[str]",
	pk_set: "Optional[Set[str]]" = None,
	fk_edges: "Optional[Sequence[FKEdge]]" = None,
	schema_id: "Optional[str]" = None,
) -> str:
	if prefix is not None and len(prefix) > 0:
		origPrefix = prefix
		prefix += "."
	else:
		origPrefix = prefix = ""

	# Avoiding special chars
	origPrefix = s_sum(origPrefix)

	origLabel = None
	ret_label: str = label
	# $label =~ s/([\[\]\{\}])/\\$1/g;

	# if kPayload.get("type", "object") == "object":
	# 	kAll = copy.copy(kPayload.get("allOf", []))
	# 	kAll.extend(kPayload.get("anyOf", []))
	# 	kAll.extend(kPayload.get("oneOf", []))
	# 	kAll.insert(0, kPayload)
	k_types = kPayload.get("type", ["object"])
	if not isinstance(k_types, list):
		k_types = [k_types]
	if "object" in k_types:
		kP, req = payloadProcessor(kPayload)

		if kP:
			origLabel = label
			if schema_id is not None:
				# See https://graphviz.org/faq/font/#what-about-svg-fonts
				ret_label = f"""
<FONT FACE="Monospace">
<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" BGCOLOR="white">
	<TR>
		<TD COLSPAN="2" ALIGN="CENTER" PORT="schema" BGCOLOR="lightgreen"><FONT POINT-SIZE="20">{html.escape(label)}</FONT><BR/><FONT POINT-SIZE="8">{html.escape(schema_id)}</FONT></TD>
	</TR>
"""
			else:
				ret_label = f"""
		<TD ALIGN="LEFT" PORT="{html.escape(origPrefix)}">{html.escape(label)}</TD>
		<TD BORDER="0"><TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
"""

			ret = []

			for keyP, valP in kP.items():
				#######valP_types = valP.setdefault("type", ["object"])
				########if valP_types is None:
				########	valP_p = simplePayloadProcessor(valP)
				########	print(f"JH {keyP}\n\n{json.dumps(valP, indent=4)}\n\n{json.dumps(valP_p, indent=4)}")
				########	valP = valP_p
				#######if not isinstance(valP_types, list):
				#######	valP_types = [valP_types]
				#######if "object" in valP_types:
				#######	valP_p , _ = payloadProcessor(valP)
				#######	valP["properties"] = valP_p
				########valP_p = simplePayloadProcessor(valP)

				valP_p = simplePayloadProcessor(valP)
				nodestr = genNode(
					keyP,
					valP_p,
					prefix,
					pk_set=pk_set,
					fk_edges=fk_edges,
					required=keyP in req,
				)
				if len(nodestr) > 0:
					ret.append(nodestr)

			if len(ret) > 0:
				ret_label += "\t<TR>\n" + "\n\t</TR>\n\t<TR>\n".join(ret) + "\t</TR>\n"

				if schema_id is not None:
					ret_label += "</TABLE></FONT>"
				else:
					ret_label += "</TABLE></TD>\n"
			elif schema_id is not None:
				ret_label += "</TABLE></FONT>"
			else:
				# No table for empty content
				ret_label = ""

	if origLabel is None:
		# label = f"\t\t<TD COLSPAN=\"2\">{label}</TD>\n"
		ret_label = ""

	return ret_label


def genNode(
	key: str,
	kPayload: "Mapping[str, Any]",
	prefix: "Optional[str]",
	pk_set: "Optional[Set[str]]" = None,
	fk_edges: "Optional[Sequence[FKEdge]]" = None,
	required: bool = False,
) -> str:
	val = key
	if prefix is None:
		prefix = ""
	while "type" in kPayload:
		k_type = kPayload["type"]
		if isinstance(k_type, list):
			k_types = k_type
		else:
			k_types = [k_type]

		s_k_types: "MutableSet[str]" = set()
		for k_t in k_types:
			s_k_t = DECO.get(k_t)
			if s_k_t is not None:
				val += s_k_t

		if ("array" in k_types) and ("object" not in k_types):
			key += "[]"

			k_items = kPayload.get("items")
			if k_items is not None:
				kPayload = k_items
				continue
		elif "object" in k_types:
			for kKey in "properties", "patternProperties", "propertyNames":
				if kKey in kPayload:
					return genObjectNodes(
						val, kPayload, prefix + key, pk_set=pk_set, fk_edges=fk_edges
					)

		break

	# Escaping
	# $val =~ s/([\[\]\{\}])/\\$1/g;

	# Avoiding special chars
	toHeaderName = s_sum(prefix + key)

	# Labelling the foreign keys
	preval = ""
	val = html.escape(val)
	if fk_edges is not None:
		for fk_edge in fk_edges:
			if fk_edge.mport == toHeaderName:
				val = f"<I>{val}</I>"
				preval += "\u2387"
				break

	if pk_set is not None and toHeaderName in pk_set:
		required = True
		val = f'<FONT COLOR="BLUE">{val}</FONT>'
		preval += "\U0001F511"

	if required:
		val = f"<B>{val}</B>"

	return f'\t\t<TD ALIGN="LEFT" PORT="{html.escape(toHeaderName)}" COLSPAN="2">{val}{preval}</TD>\n'
	# return f'\t\t<TD ALIGN="LEFT" PORT="{toHeaderName}" SIDES="LTB">{val}</TD><TD ALIGN="RIGHT" SIDES="RTB">{preval}</TD>\n'


def drawSchemasToFile(
	ev: "ExtensibleValidator", output_filename: str, title: str = "JSON Schemas"
) -> int:
	validSchemaDict = ev.getValidSchemas()
	if len(validSchemaDict.keys()) == 0:
		ev.logger.fatal("No schema was successfully loaded, so no drawing is possible")
		return 1
	with open(output_filename, mode="w", encoding="utf-8") as DOT:
		return drawSchemasToStream(ev, DOT, title=title)


def schemaPath2JSONPath(schemaPath: str) -> str:
	jpath = ""
	if len(schemaPath) == 0:
		return jpath

	prevProperties = False
	for token in schemaPath.split("/"):
		if token == "":
			continue
		if prevProperties:
			if len(jpath) > 0:
				jpath += "."
			jpath += token
			prevProperties = False
		elif token == "properties":
			prevProperties = True
		elif token == "items":
			jpath += "[]"
		else:
			module_logger.debug(f"Mira {token} {schemaPath}")

	return jpath


def drawSchemasToStream(ev: "ExtensibleValidator", DOT: "IO[str]", title: str) -> int:
	validSchemaDict = ev.getValidSchemas(do_resolve=True)
	refSchemaSet = ev.getRefSchemaSet()
	# Now it is time to draw the schemas themselves
	# See https://graphviz.org/faq/font/#what-about-svg-fonts
	pre = f"""
digraph schemas {{
	graph[ rankdir=LR, ranksep=2, fontsize=60, fontname="Sans-Serif", labelloc=t, label=< {title} <br/> <font point-size="40">(as of {datetime.datetime.now().isoformat()})</font> >  ];
	node [shape=tab, style=filled, fillcolor="green"];
	edge [penwidth=2, fontname="Serif"];
"""
	# 	node [shape=record];
	DOT.write(pre)

	# First pass
	sCounter = 0
	sHash = dict()

	for jsonSchemaURI, schemaObj in validSchemaDict.items():
		resolved_schema = schemaObj["resolved_schema"]

		for prop_name in ("properties", "allOf", "oneOf", "someOf"):
			if prop_name in resolved_schema:
				nodeId = f"s{sCounter}"
				sHash[jsonSchemaURI] = nodeId
				sCounter += 1
				break

	# Gathering edges for foreign keys
	fk_edges: "MutableSequence[FKEdge]" = []
	fk_edges_d: "MutableMapping[str, MutableSequence[FKEdge]]" = {}
	pk_sets: "MutableMapping[str, Set[str]]" = {}
	for jsonSchemaURI, jsonSchemaSet in refSchemaSet.items():
		fromNodeId = sHash.get(jsonSchemaURI)
		fromHeaderName = jsonSchemaURI
		rSlash = fromHeaderName.rfind("/")
		if rSlash != -1:
			fromHeaderName = fromHeaderName[rSlash + 1 :]
		schemaObj_o = validSchemaDict.get(jsonSchemaURI)

		if fromNodeId is None:
			continue

		if schemaObj_o is None:
			continue

		refResolver = schemaObj_o["ref_resolver"]

		id2ElemId, keyRefs, jp2val = jsonSchemaSet
		for the_id, featureLocs in keyRefs.items():
			if the_id == ForeignKey.KeyAttributeNameFK:
				# TO FINISH
				for featureLoc in featureLocs:
					for fk_decl in featureLoc.context[ForeignKey.KeyAttributeNameFK]:
						resolved = refResolver_resolve(
							refResolver, fk_decl["schema_id"]
						)
						if resolved is None:
							continue
						to_jsonSchemaURI = resolved[0]
						toHeaderName = to_jsonSchemaURI
						rSlash = toHeaderName.rfind("/")
						if rSlash != -1:
							toHeaderName = toHeaderName[rSlash + 1 :]

						toNodeId = sHash[to_jsonSchemaURI]

						the_path = featureLoc.path
						if the_path.endswith("/" + ForeignKey.KeyAttributeNameFK):
							the_path = the_path[
								0 : -(len(ForeignKey.KeyAttributeNameFK) + 1)
							]

						mport = s_sum(schemaPath2JSONPath(the_path))

						tooltip = (
							fromHeaderName
							+ "/"
							+ schemaPath2JSONPath(the_path)
							+ " -> "
							+ toHeaderName
						)
						fk_edge = FKEdge(
							fromNodeId=fromNodeId,
							mport=mport,
							toNodeId=toNodeId,
							tooltip=tooltip,
						)
						fk_edges.append(fk_edge)
						fk_edges_d.setdefault(fromNodeId, []).append(fk_edge)
			elif the_id == PrimaryKey.KeyAttributeNamePK:
				# TO FINISH
				pk_set = set()
				for featureLoc in featureLocs:
					the_path = featureLoc.path
					if the_path.endswith("/" + PrimaryKey.KeyAttributeNamePK):
						the_path = the_path[
							0 : -(len(PrimaryKey.KeyAttributeNamePK) + 1)
						]
					json_path = schemaPath2JSONPath(the_path)
					if len(json_path) > 0:
						json_path += "."
					for pk_decl in featureLoc.context[PrimaryKey.KeyAttributeNamePK]:
						pk_path = json_path + pk_decl
						mport = s_sum(pk_path)
						pk_set.add(mport)

				pk_sets[fromNodeId] = pk_set

	# First pass
	for jsonSchemaURI, schemaObj in validSchemaDict.items():
		nodeId_o = sHash.get(jsonSchemaURI)

		if nodeId_o is not None:
			resolved_schema = schemaObj["resolved_schema"]

			headerName = jsonSchemaURI
			rSlash = headerName.rfind("/")
			if rSlash != -1 and len(headerName[rSlash + 1 :]) > 0:
				headerName = headerName[rSlash + 1 :]

			label = genObjectNodes(
				headerName,
				resolved_schema,
				None,
				pk_set=pk_sets.get(nodeId_o),
				fk_edges=fk_edges_d.get(nodeId_o),
				schema_id=jsonSchemaURI,
			)

			description = resolved_schema.get(
				"description", resolved_schema.get("title", headerName)
			)

			if len(label) > 0:
				DOT.write(
					f"\t{nodeId_o} [tooltip=<{html.escape(description)}> label=<\n{label}\n>];\n"
				)

	# Second pass
	for fk_edge in fk_edges:
		customEdge = ""
		if fk_edge.fromNodeId == fk_edge.toNodeId:
			customEdge = " headport=e"

		edge_tooltip = (
			html.escape(fk_edge.tooltip).replace("[", "&#91;").replace("]", "&#93;")
		)

		DOT.write(
			f'\t{fk_edge.fromNodeId}:"{fk_edge.mport}" -> {fk_edge.toNodeId}:schema [label=<{edge_tooltip}> tooltip=<{edge_tooltip}> {customEdge}];\n'
		)

	post = """
}
"""
	DOT.write(post)

	return 0
